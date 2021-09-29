# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import subprocess
import time
import random
import shutil
import socket
import glob
import signal
from contextlib import closing

NEBULA_START_COMMAND_FORMAT = "bin/nebula-{} --flagfile conf/nebula-{}.conf {}"


class NebulaService(object):
    def __init__(self, build_dir, src_dir):
        self.build_dir = str(build_dir)
        self.src_dir = str(src_dir)
        self.work_dir = os.path.join(self.build_dir, 'server_' + time.strftime("%Y-%m-%dT%H-%M-%S", time.localtime()))
        self.pids = {}

    def set_work_dir(self, work_dir):
        self.work_dir = work_dir

    def _copy_nebula_conf(self):
        graph_path = self.build_dir + '/bin'
        graph_conf_path = self.src_dir + '/conf'
        storage_path = self.build_dir + '/bin'
        storage_conf_path = self.src_dir + '/conf'

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

        # cert files
        shutil.copy(self.src_dir + '/tests/cert/test.ca.key',
                    resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.ca.pem',
                    resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.ca.password',
                    resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.derive.key',
                    resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.derive.crt',
                    resources_dir)

    def _format_nebula_command(self, name, meta_port, ports, debug_log=True, ca_signed=False):
        params = [
            "--meta_server_addrs={}",
            "--port={}",
            "--ws_http_port={}",
            "--ws_h2_port={}",
            "--heartbeat_interval_secs=1",
            "--expired_time_factor=60",
        ]
        if ca_signed:
            params.append('--ca_path=share/resources/test.ca.pem')
            params.append('--cert_path=share/resources/test.derive.crt')
            params.append('--key_path=share/resources/test.derive.key')
        else:
            params.append('--cert_path=share/resources/test.ca.pem')
            params.append('--key_path=share/resources/test.ca.key')
            params.append('--password_path=share/resources/test.ca.password')

        if name == 'graphd':
            params.append('--local_config=false')
            params.append('--enable_authorize=true')
            params.append('--system_memory_high_watermark_ratio=0.95')
            params.append('--num_rows_to_check_memory=4')
            params.append('--session_reclaim_interval_secs=2')
        if name == 'storaged':
            params.append('--local_config=false')
            params.append('--raft_heartbeat_interval_secs=30')
            params.append('--skip_wait_in_rate_limiter=true')
        if debug_log:
            params.append('--v=4')
        param_format = " ".join(params)
        param = param_format.format("127.0.0.1:" + str(meta_port), ports[0],
                                    ports[1], ports[2])
        command = NEBULA_START_COMMAND_FORMAT.format(name, name, param)
        return command

    @staticmethod
    def is_port_in_use(port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            return s.connect_ex(('localhost', port)) == 0

    @staticmethod
    def get_free_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', random.randint(1024, 10000)))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    # TODO(yee): Find free port range
    @staticmethod
    def _find_free_port():
        # tcp_port, http_port, https_port
        ports = []
        for i in range(0, 2):
            ports.append(NebulaService.get_free_port())
        while True:
            port = NebulaService.get_free_port()
            if port not in ports and all(
                    not NebulaService.is_port_in_use(port + i)
                    for i in range(-2, 3)):
                ports.insert(0, port)
                break
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

        for i in range(0, 20):
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

    def start(self, debug_log="true", multi_graphd=False, ca_signed="false", **kwargs):
        os.chdir(self.work_dir)

        metad_ports = self._find_free_port()
        all_ports = [metad_ports[0]]
        graph_ports = []
        server_ports = []
        servers = []
        if multi_graphd:
            servers = ['metad', 'storaged', 'graphd', 'graphd1']
            os.mkdir(self.work_dir + '/logs1')
            os.mkdir(self.work_dir + '/pids1')
        else:
            servers = ['metad', 'storaged', 'graphd']
        for server_name in servers:
            ports = []
            if server_name != 'metad':
                while True:
                    ports = self._find_free_port()
                    if all((ports[0] + i) not in all_ports for i in range(-2, 3)):
                        all_ports += [ports[0]]
                        break
            else:
                ports = metad_ports
            server_ports.append(ports[0])
            new_name = server_name if server_name != 'graphd1' else 'graphd'
            command = [
                self._format_nebula_command(new_name,
                                            metad_ports[0],
                                            ports,
                                            debug_log,
                                            ca_signed=ca_signed)
            ]
            if server_name == 'graphd1':
                command.append('--log_dir=logs1')
                command.append('--pid_file=pids1/nebula-graphd.pid')

            for k,v in kwargs.items():
                command.append("--{}={}".format(k, v))

            cmd = " ".join(command)
            print("exec: " + cmd)
            p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
            p.wait()
            if p.returncode != 0:
                print("error: " + bytes.decode(p.communicate()[0]))
            elif server_name.find('graphd') != -1:
                graph_ports.append(ports[0])

        # wait nebula start
        start_time = time.time()
        if not self._check_servers_status(server_ports):
            self._collect_pids()
            self.kill_all(signal.SIGKILL)
            elapse = time.time() - start_time
            raise Exception(f'nebula servers not ready in {elapse}s')

        self._collect_pids()

        return graph_ports

    def _collect_pids(self):
        for pf in glob.glob(self.work_dir + '/pids/*.pid'):
            with open(pf) as f:
                self.pids[f.name] = int(f.readline())
        for pf in glob.glob(self.work_dir + '/pids1/*.pid'):
            with open(pf) as f:
                self.pids[f.name] = int(f.readline())

    def stop(self, cleanup):
        print("try to stop nebula services...")
        self._collect_pids()
        self.kill_all(signal.SIGTERM)

        max_retries = 20
        while self.is_procs_alive() and max_retries >= 0:
            time.sleep(1)
            max_retries = max_retries-1

        self.kill_all(signal.SIGKILL)

        if cleanup:
            shutil.rmtree(self.work_dir, ignore_errors=True)

    def kill_all(self, sig):
        for p in self.pids:
            self.kill(p, sig)

    def kill(self, pid, sig):
        if not self.is_proc_alive(pid):
            return
        try:
            os.kill(self.pids[pid], sig)
        except OSError as err:
            print("stop nebula {} failed: {}".format(pid, str(err)))

    def is_procs_alive(self):
        return any(self.is_proc_alive(pid) for pid in self.pids)

    def is_proc_alive(self, pid):
        process = subprocess.Popen(['ps', '-eo', 'pid,args'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout = process.communicate()
        for line in bytes.decode(stdout[0]).splitlines():
            p = line.lstrip().split(' ', 1)[0]
            if str(p) == str(self.pids[pid]):
                return True
        return False
