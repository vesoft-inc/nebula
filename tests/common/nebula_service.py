# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import random
import subprocess
import time
import shutil
import socket
import glob
import signal
from contextlib import closing

NEBULA_START_COMMAND_FORMAT = "bin/nebula-{} --flagfile conf/nebula-{}.conf {}"


class NebulaService(object):
    def __init__(self, build_dir, src_dir):
        self.build_dir = build_dir
        self.src_dir = src_dir
        self.work_dir = os.path.join(self.build_dir, 'server_' + time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()))
        self.pids = {}

    def set_work_dir(self, work_dir):
        self.work_dir = work_dir

    def _copy_nebula_conf(self):
        graph_path = self.build_dir + '/bin'
        graph_conf_path = self.src_dir + '/conf'
        storage_path = self.src_dir + '/build/modules/storage/bin'
        storage_conf_path = self.src_dir + '/modules/storage/conf'

        # graph
        shutil.copy(graph_path + '/nebula-graphd', self.work_dir + '/bin/')
        shutil.copy(graph_conf_path + '/nebula-graphd.conf.default',
                    self.work_dir + '/conf/nebula-graphd.conf')
        # storage
        shutil.copy(storage_path + '/nebula-storaged', self.work_dir + '/bin/')
        shutil.copy(storage_conf_path + '/nebula-storaged.conf.default',
                    self.work_dir + '/conf/nebula-storaged.conf')
        # meta
        shutil.copy(storage_path + '/nebula-metad', self.work_dir + '/bin/')
        shutil.copy(storage_conf_path + '/nebula-metad.conf.default',
                    self.work_dir + '/conf/nebula-metad.conf')

        # gflags.json
        resources_dir = self.work_dir + '/share/resources/'
        os.makedirs(resources_dir)
        shutil.copy(self.build_dir + '/../resources/gflags.json', resources_dir)

    def _format_nebula_command(self, name, meta_port, ports, debug_log=True):
        param_format = "--meta_server_addrs={} --port={} --ws_http_port={} --ws_h2_port={} --heartbeat_interval_secs=1"
        param = param_format.format("127.0.0.1:" + str(meta_port), ports[0],
                                    ports[1], ports[2])
        if name == 'graphd':
            param += ' --enable_optimizer=true'
            param += ' --enable_authorize=true'
        if name == 'storaged':
            param += ' --raft_heartbeat_interval_secs=30'
        if debug_log:
            param += ' --v=4'
        command = NEBULA_START_COMMAND_FORMAT.format(name, name, param)
        return command

    def _find_free_port(self):
        ports = []
        for i in range(0, 3):
            with closing(socket.socket(socket.AF_INET,
                                       socket.SOCK_STREAM)) as s:
                s.bind(('', 0))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                ports.append(s.getsockname()[1])
        return ports

    def _telnet_port(self, port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sk:
            sk.settimeout(1)
            result = sk.connect_ex(('127.0.0.1', port))
            return result == 0

    def install(self):
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir)
        os.mkdir(self.work_dir)
        print("work directory: " + self.work_dir)
        os.chdir(self.work_dir)
        installed_files = ['logs', 'bin', 'conf', 'data', 'pids', 'scripts']
        for f in installed_files:
            os.mkdir(self.work_dir + '/' + f)
        self._copy_nebula_conf()

    def _check_servers_status(self, ports):
        ports_status = {}
        for port in ports:
            ports_status[port] = False

        for i in range(0, 30):
            for port in ports_status:
                if ports_status[port]:
                    continue
                if self._telnet_port(port):
                    ports_status[port] = True
            is_ok = True
            for port in ports_status:
                if not ports_status[port]:
                    is_ok = False
            if is_ok:
                return True
            time.sleep(1)
        return False

    def start(self, debug_log=True):
        os.chdir(self.work_dir)

        metad_ports = self._find_free_port()
        command = ''
        storage_port = 0
        graph_port = 0
        server_ports = []
        for server_name in ['metad', 'storaged', 'graphd']:
            ports = []
            if server_name != 'metad':
                ports = self._find_free_port()
            else:
                ports = metad_ports
            server_ports.append(ports[0])
            if server_name == 'storaged':
                storage_port = ports[0]
            command = self._format_nebula_command(server_name,
                                                  metad_ports[0],
                                                  ports,
                                                  debug_log)
            print("exec: " + command)
            p = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE)
            p.wait()
            if p.returncode != 0:
                print("error: " + bytes.decode(p.communicate()[0]))
            else:
                graph_port = ports[0]

        # wait nebula start
        start_time = time.time()
        if not self._check_servers_status(server_ports):
            raise Exception('nebula servers not ready in {}s'.format(time.time() - start_time))
        print('nebula servers start ready in {}s'.format(time.time() - start_time))

        for pf in glob.glob(self.work_dir + '/pids/*.pid'):
            with open(pf) as f:
                self.pids[f.name] = int(f.readline())

        return (storage_port, graph_port)

    def stop(self, cleanup):
        print("try to stop nebula services...")
        for p in self.pids:
            try:
                os.kill(self.pids[p], signal.SIGTERM)
            except OSError as err:
                print("nebula stop {} failed: {}".format(p, str(err)))

        max_retries = 30
        while self.check_procs_alive() and max_retries >= 0:
            time.sleep(1)
            max_retries = max_retries-1

        if cleanup:
            shutil.rmtree(self.work_dir, ignore_errors=True)

    def check_procs_alive(self):
        process = subprocess.Popen(['ps', '-eo', 'pid,args'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout = process.communicate()
        for line in bytes.decode(stdout[0]).splitlines():
            pid = line.lstrip().split(' ', 1)[0]
            for p in self.pids:
                if str(self.pids[p]) == str(pid):
                    return True
        return False
