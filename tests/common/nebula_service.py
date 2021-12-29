# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import os
import subprocess
import time
import random
import shutil
import socket
import glob
import signal
import copy
import fcntl
import logging
from pathlib import Path
from contextlib import closing

from tests.common.constants import TMP_DIR
from tests.common.utils import get_ssl_config
from tests.common.logger import logger
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

NEBULA_START_COMMAND_FORMAT = "bin/nebula-{} --flagfile conf/nebula-{}.conf {}"


class NebulaProcess(object):
    def __init__(self, name, ports, suffix_index=0, params=None):
        if params is None:
            params = {}
        assert len(ports) == 4, 'should have 4 ports but have {}'.format(len(ports))
        self.name = name
        self.tcp_port, self.tcp_internal_port, self.http_port, self.https_port = ports
        self.suffix_index = suffix_index
        self.params = params
        self.host = '127.0.0.1'
        self.pid = None

    def update_param(self, params):
        self.params.update(params)

    def update_meta_server_addrs(self, address):
        self.update_param({'meta_server_addrs': address})

    def _format_nebula_command(self):
        process_params = {
            'log_dir': 'logs{}'.format(self.suffix_index),
            'pid_file': 'pids{}/nebula-{}.pid'.format(self.suffix_index, self.name),
            'port': self.tcp_port,
            'ws_http_port': self.http_port,
            'ws_h2_port': self.https_port,
        }
        # data path
        if self.name.upper() != 'GRAPHD':
            process_params['data_path'] = 'data{}/{}'.format(
                self.suffix_index, self.name
            )

        process_params.update(self.params)
        cmd = [
            'bin/nebula-{}'.format(self.name),
            '--flagfile',
            'conf/nebula-{}.conf'.format(self.name),
        ] + ['--{}={}'.format(key, value) for key, value in process_params.items()]

        return " ".join(cmd)

    def start(self):
        cmd = self._format_nebula_command()
        logger.info("exec: " + cmd)
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
        p.wait()
        if p.returncode != 0:
            logger.info("start process error: " + bytes.decode(p.communicate()[0]))
        proc1 = subprocess.Popen(['ps', 'x'], stdout=subprocess.PIPE)
        proc2 = subprocess.Popen(['grep', str(self.tcp_port)], stdin=proc1.stdout,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc1.stdout.close()
        stdout, _ = proc2.communicate()
        pid = None
        for line in bytes.decode(stdout).splitlines():
            pid = line.lstrip().split(' ')[0]
            break
        assert pid is not None
        self.pid = int(pid)

    def kill(self, sig):
        if not self.is_alive():
            return
        try:
            os.kill(self.pid, sig)
        except OSError as err:
            logger.info(
                "stop nebula-{} {} failed: {}".format(self.name, self.pid, str(err))
            )

    def force_kill(self, timeout=3):
        for _ in range(timeout):
            self.kill(signal.SIGTERM)
            if not self.is_alive():
                break
            time.sleep(1)
        if self.is_alive():
            self.kill(signal.SIGKILL)

    def is_alive(self):
        if self.pid is None:
            return False

        process = subprocess.Popen(
            ['ps', '-eo', 'pid,args'], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout = process.communicate()
        for line in bytes.decode(stdout[0]).splitlines():
            p = line.lstrip().split(' ', 1)[0]
            if str(p) == str(self.pid):
                return True
        return False


class NebulaService(object):
    def __init__(
        self,
        build_dir,
        src_dir,
        metad_num=1,
        storaged_num=1,
        graphd_num=1,
        ca_signed=False,
        debug_log=True,
        **kwargs,
    ):
        assert graphd_num > 0 and metad_num > 0 and storaged_num > 0
        self.build_dir = str(build_dir)
        self.src_dir = str(src_dir)
        self.work_dir = os.path.join(
            self.build_dir,
            'server_' + time.strftime('%Y-%m-%dT%H-%M-%S', time.localtime()),
        )
        self.pids = {}
        self.metad_num, self.storaged_num, self.graphd_num = (
            metad_num,
            storaged_num,
            graphd_num,
        )
        self.metad_processes, self.storaged_processes, self.graphd_processes = (
            [],
            [],
            [],
        )
        self.all_processes = []
        self.all_ports = []
        self.metad_param, self.storaged_param, self.graphd_param = {}, {}, {}
        self.storaged_port = 0
        self.graphd_port = 0
        self.ca_signed = ca_signed
        self.is_graph_ssl = (
            kwargs.get("enable_graph_ssl", "false").upper() == "TRUE"
            or kwargs.get("enable_ssl", "false").upper() == "TRUE"
        )

        self.debug_log = debug_log
        self.ports_per_process = 4
        self.lock_file = os.path.join(TMP_DIR, "cluster_port.lock")
        self.delimiter = "\n"
        self._make_params(**kwargs)
        self.init_process()

    def init_process(self):
        process_count = self.metad_num + self.storaged_num + self.graphd_num
        ports_count = process_count * self.ports_per_process
        self.all_ports = self._find_free_port(ports_count)
        index = 0

        for suffix_index in range(self.metad_num):
            metad = NebulaProcess(
                "metad",
                self.all_ports[index : index + self.ports_per_process],
                suffix_index,
                self.metad_param,
            )
            self.metad_processes.append(metad)
            index += self.ports_per_process

        for suffix_index in range(self.storaged_num):
            storaged = NebulaProcess(
                "storaged",
                self.all_ports[index : index + self.ports_per_process],
                suffix_index,
                self.storaged_param,
            )
            self.storaged_processes.append(storaged)
            index += self.ports_per_process
            if suffix_index == 0:
                self.storaged_port = self.all_ports[0]

        for suffix_index in range(self.graphd_num):
            graphd = NebulaProcess(
                "graphd",
                self.all_ports[index : index + self.ports_per_process],
                suffix_index,
                self.graphd_param,
            )
            self.graphd_processes.append(graphd)
            index += self.ports_per_process
            if suffix_index == 0:
                self.graphd_port = self.all_ports[0]

        self.all_processes = (
            self.metad_processes + self.storaged_processes + self.graphd_processes
        )
        # update meta address
        meta_server_addrs = ','.join(
            [
                '{}:{}'.format(process.host, process.tcp_port)
                for process in self.metad_processes
            ]
        )

        for p in self.all_processes:
            p.update_meta_server_addrs(meta_server_addrs)

    def _make_params(self, **kwargs):
        _params = {
            'heartbeat_interval_secs': 1,
            'expired_time_factor': 60,
        }
        if self.ca_signed:
            _params['cert_path'] = 'share/resources/test.derive.crt'
            _params['key_path'] = 'share/resources/test.derive.key'
            _params['ca_path'] = 'share/resources/test.ca.pem'

        else:
            _params['cert_path'] = 'share/resources/test.ca.pem'
            _params['key_path'] = 'share/resources/test.ca.key'
            _params['password_path'] = 'share/resources/test.ca.password'

        if self.debug_log:
            _params['v'] = '4'

        self.graphd_param = copy.copy(_params)
        self.graphd_param['local_config'] = 'false'
        self.graphd_param['enable_authorize'] = 'true'
        self.graphd_param['system_memory_high_watermark_ratio'] = '0.95'
        self.graphd_param['num_rows_to_check_memory'] = '4'
        self.graphd_param['session_reclaim_interval_secs'] = '2'
        # Login retry
        self.graphd_param['failed_login_attempts'] = '5'
        self.graphd_param['password_lock_time_in_secs'] = '10'

        self.storaged_param = copy.copy(_params)
        self.storaged_param['local_config'] = 'false'
        self.storaged_param['raft_heartbeat_interval_secs'] = '30'
        self.storaged_param['skip_wait_in_rate_limiter'] = 'true'
        self.metad_param = copy.copy(_params)
        for p in [self.metad_param, self.storaged_param, self.graphd_param]:
            p.update(kwargs)

    def set_work_dir(self, work_dir):
        self.work_dir = work_dir

    def _copy_nebula_conf(self):
        bin_path = self.build_dir + '/bin/'
        conf_path = self.src_dir + '/conf/'

        for item in ['nebula-graphd', 'nebula-storaged', 'nebula-metad']:
            shutil.copy(bin_path + item, self.work_dir + '/bin/')
            shutil.copy(
                conf_path + '{}.conf.default'.format(item),
                self.work_dir + '/conf/{}.conf'.format(item),
            )

        resources_dir = self.work_dir + '/share/resources/'
        os.makedirs(resources_dir)

        # timezone file
        shutil.copy(
            self.build_dir + '/../resources/date_time_zonespec.csv', resources_dir
        )
        shutil.copy(self.build_dir + '/../resources/gflags.json', resources_dir)
        # cert files
        shutil.copy(self.src_dir + '/tests/cert/test.ca.key', resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.ca.pem', resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.ca.password', resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.derive.key', resources_dir)
        shutil.copy(self.src_dir + '/tests/cert/test.derive.crt', resources_dir)

    @staticmethod
    def is_port_in_use(port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            return s.connect_ex(('localhost', port)) == 0

    @staticmethod
    def get_free_port():
        for _ in range(30):
            try:
                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                    s.bind(('', random.randint(10000, 20000)))
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    return s.getsockname()[1]
            except OSError as e:
                pass

    # TODO(yee): Find free port range
    def _find_free_port(self, count):
        assert count % self.ports_per_process == 0
        Path(self.lock_file).touch(exist_ok=True)
        # thread safe
        with open(self.lock_file, 'r+') as fl:
            fcntl.flock(fl.fileno(), fcntl.LOCK_EX)
            context = fl.read().strip()
            lock_ports = [int(p) for p in context.split(self.delimiter) if p != ""]

            all_ports = []
            for i in range(count):
                if i % self.ports_per_process == 0:
                    for _ in range(100):
                        tcp_port = NebulaService.get_free_port()
                        # force internal tcp port with port+1
                        if all(
                            (tcp_port + i) not in all_ports + lock_ports
                            for i in range(0, 2)
                        ):
                            all_ports.append(tcp_port)
                            all_ports.append(tcp_port + 1)
                            break

                elif i % self.ports_per_process == 1:
                    continue
                else:
                    for _ in range(100):
                        port = NebulaService.get_free_port()
                        if port not in all_ports + lock_ports:
                            all_ports.append(port)
                            break
            fl.seek(0)
            fl.truncate()

            fl.write(self.delimiter.join([str(p) for p in all_ports + lock_ports]))
            fl.write(self.delimiter)

        return all_ports

    def _telnet_port(self, port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sk:
            sk.settimeout(1)
            result = sk.connect_ex(('127.0.0.1', port))
            return result == 0

    def install(self, work_dir=None):
        if work_dir is not None:
            self.work_dir = work_dir
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir)
        Path(self.work_dir).mkdir(exist_ok=True)
        logger.info("work directory: " + self.work_dir)
        os.chdir(self.work_dir)
        installed_files = ['bin', 'conf', 'scripts']
        for f in installed_files:
            Path(self.work_dir + '/' + f).mkdir(exist_ok=True)
        self._copy_nebula_conf()
        max_suffix = max([self.graphd_num, self.storaged_num, self.metad_num])
        for i in range(max_suffix):
            Path(self.work_dir + '/logs{}'.format(i)).mkdir(exist_ok=True)
            Path(self.work_dir + '/pids{}'.format(i)).mkdir(exist_ok=True)

    def _check_servers_status(self, ports, times=20):
        failed_ports = set()
        for port in ports:
            failed_ports.add(port)

        for i in range(0, times):
            if len(failed_ports) == 0:
                break
            ports = copy.copy(failed_ports)
            for port in ports:
                if not self._telnet_port(port):
                    continue
                if port in failed_ports:
                    failed_ports.remove(port)

            time.sleep(1)

        return len(failed_ports) == 0, failed_ports

    def start(self):
        os.chdir(self.work_dir)
        start_time = time.time()

        for p in self.metad_processes:
            p.start()
        # if metad is not ready, graphd would exit after 3 heartbeat.
        # so start metad, and then wait for 2 seconds
        for _ in range(10):
            if all([self.is_port_in_use(p.tcp_port) for p in self.metad_processes]):
                break
            time.sleep(1)

        for p in self.storaged_processes + self.graphd_processes:
            p.start()

        config = Config()
        config.max_connection_pool_size = 20
        config.timeout = 60000
        # init connection pool
        client_pool = ConnectionPool()
        # assert client_pool.init([("127.0.0.1", int(self.graphd_port))], config)
        ssl_config = get_ssl_config(self.is_graph_ssl, self.ca_signed)
        logger.info("begin to add hosts")
        ok = False
        # wait graph is ready, and then add hosts
        for _ in range(20):
            try:
                ok = client_pool.init(
                    [("127.0.0.1", self.graphd_processes[0].tcp_port)],
                    config,
                    ssl_config,
                )
                if ok:
                    break
            except:
                pass
            time.sleep(1)

        assert ok, "graph is not ready"
        # get session from the pool
        client = client_pool.get_session('root', 'nebula')

        hosts = ",".join(
            [
                "127.0.0.1:{}".format(str(storaged.tcp_port))
                for storaged in self.storaged_processes
            ]
        )
        cmd = "ADD HOSTS {} INTO NEW ZONE \"default_zone\"".format(hosts)
        logger.info("add hosts cmd is {}".format(cmd))
        resp = client.execute(cmd)
        assert resp.is_succeeded(), resp.error_msg()
        client.release()

        # wait nebula start
        server_ports = [p.tcp_port for p in self.all_processes]
        status, failed_ports = self._check_servers_status(server_ports)
        if not status:
            self._collect_pids()
            self.kill_all(signal.SIGKILL)
            elapse = time.time() - start_time
            raise Exception(
                'nebula servers not ready in {}s, failed ports are {}'.format(
                    elapse, failed_ports
                )
            )

        self._collect_pids()

        return [p.tcp_port for p in self.graphd_processes]

    def _collect_pids(self):
        for pf in glob.glob(self.work_dir + '/pid*/*.pid'):
            with open(pf) as f:
                self.pids[f.name] = int(f.readline())

    def stop(self, cleanup=True):
        logger.info("try to stop nebula services...")
        self._collect_pids()
        if len(self.pids) == 0:
            logger.info("the cluster has been stopped and deleted.")
            return
        self.kill_all(signal.SIGTERM)

        max_retries = 10
        while self.is_procs_alive() and max_retries >= 0:
            time.sleep(1)
            max_retries = max_retries - 1

        if self.is_procs_alive():
            logger.warning(
                "cannot stop process via sigterm, alived processes are {}, force kill!".format(
                    self.alive_procs()
                )
            )
            self.kill_all(signal.SIGKILL)

        # thread safe
        with open(self.lock_file, 'r+') as fl:
            fcntl.flock(fl.fileno(), fcntl.LOCK_EX)
            context = fl.read().strip()
            lock_ports = {int(p) for p in context.split(self.delimiter) if p != ""}
            for p in self.all_ports:
                lock_ports.remove(p)
            fl.seek(0)
            fl.truncate()
            fl.write(self.delimiter.join([str(p) for p in lock_ports]))
            fl.write(self.delimiter)

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
            logger.info("stop nebula {} failed: {}".format(pid, str(err)))

    def is_procs_alive(self):
        return len(self.alive_procs()) != 0

    def alive_procs(self):
        self._collect_pids()
        procs = []
        for pid in self.pids:
            if self.is_proc_alive(pid):
                procs.append(pid)

        return procs

    def is_proc_alive(self, pid):
        process = subprocess.Popen(
            ['ps', '-eo', 'pid,args'], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout = process.communicate()
        for line in bytes.decode(stdout[0]).splitlines():
            p = line.lstrip().split(' ', 1)[0]
            if str(p) == str(self.pids[pid]):
                return True
        return False

    def add_process(self, process_type, params=None):
        if params is None:
            params = {}
        process = None
        if process_type.lower() not in ["graphd", "graph", "storaged", "storage"]:
            raise Exception("process type could only be graphd or storaged")

        ports = self._find_free_port(4)

        if process_type.lower() in ["graphd", "graph"]:
            original_params = self.graphd_processes[0].params
            params.update(original_params)
            index = self.graphd_num
            process = NebulaProcess("graphd", ports, index, params)
            self.graphd_num += 1
            self.graphd_processes.append(process)
            self.all_processes.append(process)

        elif process_type.lower() in ["storaged", "storage"]:
            original_params = self.storaged_processes[0].params
            params.update(original_params)
            index = self.storaged_num
            process = NebulaProcess("storaged", ports, index, params)
            self.storaged_num += 1
            self.storaged_processes.append(process)
            self.all_processes.append(process)

        Path(self.work_dir + '/logs{}'.format(index)).mkdir(exist_ok=True)
        Path(self.work_dir + '/pids{}'.format(index)).mkdir(exist_ok=True)
