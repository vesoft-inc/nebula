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
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

NEBULA_START_COMMAND_FORMAT = "bin/nebula-{} --flagfile conf/nebula-{}.conf {}"


class NebulaProcess(object):
    def __init__(self, name, ports, suffix_index=0, params=None, is_standalone=False):
        self.is_sa = is_standalone
        if params is None:
            params = {}
        if is_standalone == False:
            assert len(ports) == 4, 'should have 4 ports but have {}'.format(len(ports))
            self.name = name
            self.tcp_port, self.tcp_internal_port, self.http_port, self.https_port = ports
        else:
            assert len(ports) == 12, 'should have 12 ports but have {}'.format(len(ports))
            self.name = name
            self.tcp_port, self.tcp_internal_port, self.http_port, self.https_port = ports[0:4]
            self.meta_port, self.meta_tcp_internal_port, self.meta_http_port, self.meta_https_port = ports[4:8]
            self.storage_port, self.storage_tcp_internal_port, self.storage_http_port, self.storage_https_port = ports[8:12]
        self.suffix_index = suffix_index
        self.params = params
        self.host = '127.0.0.1'
        self.pid = None

    def update_param(self, params):
        self.params.update(params)

    def update_meta_server_addrs(self, address):
        self.update_param({'meta_server_addrs': address})

    def _format_nebula_command(self):
        if self.is_sa == False:
            process_params = {
                'log_dir': 'logs{}'.format(self.suffix_index),
                'pid_file': 'pids{}/nebula-{}.pid'.format(self.suffix_index, self.name),
                'port': self.tcp_port,
                'ws_http_port': self.http_port,
            }
        else:
            process_params = {
                'log_dir': 'logs{}'.format(self.suffix_index),
                'pid_file': 'pids{}/nebula-{}.pid'.format(self.suffix_index, self.name),
                'port': self.tcp_port,
                'ws_http_port': self.http_port,
                'meta_port': self.meta_port,
                'ws_meta_http_port': self.meta_http_port,
                'storage_port': self.storage_port,
                'ws_storage_http_port': self.storage_http_port,
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
        print("exec: " + cmd)
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
        p.wait()
        if p.returncode != 0:
            print("error: " + bytes.decode(p.communicate()[0]))
        self.pid = p.pid

    def kill(self, sig):
        if not self.is_alive():
            return
        try:
            os.kill(self.pid, sig)
        except OSError as err:
            print("stop nebula-{} {} failed: {}".format(self.name, self.pid, str(err)))

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
        use_standalone=False,
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

        if use_standalone == False:
            self._make_params(**kwargs)
            self.init_process()
        else:
            self._make_sa_params(**kwargs)
            self.init_standalone()

    def init_standalone(self):
        process_count = self.metad_num + self.storaged_num + self.graphd_num
        ports_count = process_count * self.ports_per_process
        self.all_ports = self._find_free_port(ports_count)
        print(self.all_ports)
        index = 0
        standalone = NebulaProcess(
            "standalone",
            self.all_ports[index : index + ports_count ],
            index,
            self.graphd_param,
            is_standalone=True
        )
        self.graphd_processes.append(standalone)
        self.all_processes = (
            self.graphd_processes
        )
        # update meta address
        meta_server_addrs = ','.join(
            [
                '{}:{}'.format(process.host, process.meta_port)
                for process in self.graphd_processes
            ]
        )

        for p in self.all_processes:
            p.update_meta_server_addrs(meta_server_addrs)

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
        # expression depth limit
        self.graphd_param['max_expression_depth'] = '128'

        self.storaged_param = copy.copy(_params)
        self.storaged_param['local_config'] = 'false'
        self.storaged_param['raft_heartbeat_interval_secs'] = '30'
        self.storaged_param['skip_wait_in_rate_limiter'] = 'true'
        self.metad_param = copy.copy(_params)
        for p in [self.metad_param, self.storaged_param, self.graphd_param]:
            p.update(kwargs)

    def _make_sa_params(self, **kwargs):
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
        self.graphd_param['raft_heartbeat_interval_secs'] = '30'
        self.graphd_param['skip_wait_in_rate_limiter'] = 'true'
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

    def _copy_standalone_conf(self):
        bin_path = self.build_dir + '/bin/'
        conf_path = self.src_dir + '/conf/'

        for item in ['nebula-standalone']:
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

    def install_standalone(self, work_dir=None):
        if work_dir is not None:
            self.work_dir = work_dir
            print("workdir not exist")
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir)
        os.mkdir(self.work_dir)
        print("work directory: " + self.work_dir)
        os.chdir(self.work_dir)
        installed_files = ['bin', 'conf', 'scripts']
        for f in installed_files:
            os.mkdir(self.work_dir + '/' + f)
        self._copy_standalone_conf()
        max_suffix = max([self.graphd_num, self.storaged_num, self.metad_num])
        for i in range(max_suffix):
            os.mkdir(self.work_dir + '/logs{}'.format(i))
            os.mkdir(self.work_dir + '/pids{}'.format(i))

    def install(self, work_dir=None):
        if work_dir is not None:
            self.work_dir = work_dir
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir)
        os.mkdir(self.work_dir)
        print("work directory: " + self.work_dir)
        os.chdir(self.work_dir)
        installed_files = ['bin', 'conf', 'scripts']
        for f in installed_files:
            os.mkdir(self.work_dir + '/' + f)
        self._copy_nebula_conf()
        max_suffix = max([self.graphd_num, self.storaged_num, self.metad_num])
        for i in range(max_suffix):
            os.mkdir(self.work_dir + '/logs{}'.format(i))
            os.mkdir(self.work_dir + '/pids{}'.format(i))

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

    def start(self):
        os.chdir(self.work_dir)
        start_time = time.time()
        for p in self.all_processes:
            p.start()

        config = Config()
        config.max_connection_pool_size = 20
        config.timeout = 60000
        # init connection pool
        client_pool = ConnectionPool()
        # assert client_pool.init([("127.0.0.1", int(self.graphd_port))], config)
        ssl_config = get_ssl_config(self.is_graph_ssl, self.ca_signed)
        print("begin to add hosts")
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
        cmd = "ADD HOSTS {}".format(hosts)
        print("add hosts cmd is {}".format(cmd))
        resp = client.execute(cmd)
        assert resp.is_succeeded(), resp.error_msg()
        client.release()

        # wait nebula start
        server_ports = [p.tcp_port for p in self.all_processes]
        if not self._check_servers_status(server_ports):
            self._collect_pids()
            self.kill_all(signal.SIGKILL)
            elapse = time.time() - start_time
            raise Exception(f'nebula servers not ready in {elapse}s')

        self._collect_pids()

        return [p.tcp_port for p in self.graphd_processes]

    def start_standalone(self):
        os.chdir(self.work_dir)
        start_time = time.time()
        for p in self.all_processes:
            print('start stand alone process')
            p.start()

        config = Config()
        config.max_connection_pool_size = 20
        config.timeout = 60000
        # init connection pool
        client_pool = ConnectionPool()
        # assert client_pool.init([("127.0.0.1", int(self.graphd_port))], config)
        ssl_config = get_ssl_config(self.is_graph_ssl, self.ca_signed)
        print("begin to add hosts")
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
                "127.0.0.1:{}".format(str(storaged.storage_port))
                for storaged in self.graphd_processes
            ]
        )
        cmd = "ADD HOSTS {}".format(hosts)
        print("add hosts cmd is {}".format(cmd))
        resp = client.execute(cmd)
        assert resp.is_succeeded(), resp.error_msg()
        client.release()

        # wait nebula start
        server_ports = [p.tcp_port for p in self.all_processes]
        if not self._check_servers_status(server_ports):
            self._collect_pids()
            self.kill_all(signal.SIGKILL)
            elapse = time.time() - start_time
            raise Exception(f'nebula servers not ready in {elapse}s')

        self._collect_pids()

        return [p.tcp_port for p in self.graphd_processes]

    def _collect_pids(self):
        for pf in glob.glob(self.work_dir + '/pid*/*.pid'):
            with open(pf) as f:
                self.pids[f.name] = int(f.readline())

    def stop(self, cleanup=True):
        print("try to stop nebula services...")
        self._collect_pids()
        if len(self.pids) == 0:
            print("the cluster has been stopped and deleted.")
            return
        self.kill_all(signal.SIGTERM)

        max_retries = 20
        while self.is_procs_alive() and max_retries >= 0:
            time.sleep(1)
            max_retries = max_retries - 1

        if self.is_procs_alive():
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
            print("stop nebula {} failed: {}".format(pid, str(err)))

    def is_procs_alive(self):
        return any(self.is_proc_alive(pid) for pid in self.pids)

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
