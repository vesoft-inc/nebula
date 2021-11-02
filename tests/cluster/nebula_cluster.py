#!/usr/bin/python3
# coding: utf-8

import os
import signal
import json
import time
import socket
import shutil
import tempfile
import subprocess
import traceback
from pathlib import Path

def check_process_stopped(pid_dir):
    pidfs = os.listdir(pid_dir)
    pids = []
    for pidf in pidfs:
        with open(os.path.join(pid_dir, pidf)) as f:
            pid = int(f.read())
            pids.append(pid)

    for pid in pids:
        try:
            os.kill(pid, 0)
        except OSError:
            # ignore
            pass
        else:
            return False

    return True

def stop_process(pid_file, signo=signal.SIGTERM):
    with open(pid_file) as fp:
        pid = int(fp.read())

    try:
        print('killing {}'.format(pid))
        os.kill(pid, signo)
        return True
    except OSError:
        print('os error')
        print('failed killing {}'.format(pid))
        traceback.print_exc()
        return False
    except:
        print('error')
        return False



def check_process_alive(self, pid):
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    else:
        return True

def assign_port(port=0, host=''):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()

        return port
    except Exception:
        return None

def check_port(port, host='127.0.0.1', wait=1, retry=3):
    def _check():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex((host, port)) == 0

    for _ in range(retry):
        if _check():
            return True

        time.sleep(wait)

    return False

class NebulaManager(object):
    # currently only one meta supported
    def __init__(self, build_dir, cluster_dir=None, meta_num=1, storage_num=1, graph_num=1, local_ip='127.0.0.1'):
        self.build_dir = build_dir
        self.nebula_dir = os.path.dirname(self.build_dir)
        self.default_conf_dir = os.path.join(self.nebula_dir, 'conf')
        self.cluster_dir = cluster_dir
        self.meta_num = meta_num
        self.storage_num = storage_num
        self.graph_num = graph_num
        self.local_ip = local_ip
        # self.graph_port_list = [9669, 9009, 9119, 9229, 9339]

        if self.cluster_dir is None:
            fd, self.cluster_dir = tempfile.mkstemp(prefix='nebula-test')
            # we don't need fd
            os.close(fd)

        self.bin_dir = os.path.join(self.build_dir, 'bin')
        self.data_dir = os.path.join(self.cluster_dir, 'data')
        self.etc_dir = os.path.join(self.cluster_dir, 'etc')
        self.logs_dir = os.path.join(self.cluster_dir, 'logs')
        self.pids_dir = os.path.join(self.cluster_dir, 'pids')
        self.meta_ports = {}
        self.graph_ports = {}
        self.storage_ports = {}

    def __enter__(self):
        self.prepare()
        return self

    def __exit__(self, type, value, trace):
        self.cleanup()

    def check_cluster_stopped(self):
        return check_process_stopped(self.pids_dir)

    def prepare(self):
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.etc_dir).mkdir(parents=True, exist_ok=True)
        Path(self.logs_dir).mkdir(parents=True, exist_ok=True)
        Path(self.pids_dir).mkdir(parents=True, exist_ok=True)

        default_meta_conf_file = os.path.join(self.default_conf_dir, 'nebula-metad.conf.production')
        meta_conf_file = os.path.join(self.etc_dir, 'nebula-metad.conf')
        shutil.copy2(default_meta_conf_file, meta_conf_file)

        default_storage_conf_file = os.path.join(self.default_conf_dir, 'nebula-storaged.conf.production')
        storage_conf_file = os.path.join(self.etc_dir, 'nebula-storaged.conf')
        shutil.copy2(default_storage_conf_file, storage_conf_file)

        default_graph_conf_file = os.path.join(self.default_conf_dir, 'nebula-graphd.conf.production')
        graph_conf_file = os.path.join(self.etc_dir, 'nebula-graphd.conf')
        shutil.copy2(default_graph_conf_file, graph_conf_file)

        self.try_load_cluster_info()
        self.start_meta()
        self.start_storage()
        self.start_graph()
        self.dump_cluster_info()

    def try_load_cluster_info(self):
        cluster_info_file = os.path.join(self.cluster_dir, 'clusterinfo.json')
        try:
            with open(cluster_info_file, 'r') as fp:
                cluster_info = json.load(fp)
                self.meta_ports = cluster_info['meta_info']
                self.graph_ports = cluster_info['graph_info']
                self.storage_ports = cluster_info['storge_info']
        except:
            pass

        if len(self.graph_ports) == 0:
            self.graph_ports['0'] = 9669
            self.graph_ports['1'] = 9009
            self.graph_ports['2'] = 9119

        if len(self.meta_ports) == 0:
            self.meta_ports['0'] = 9559
            # self.meta_ports['1'] = 9229
            # self.meta_ports['2'] = 9339

    def dump_cluster_info(self):
        with open(os.path.join(self.cluster_dir, 'clusterinfo.json'), 'w') as fp:
            clusterinfo = {
                'meta_info': self.meta_ports,
                'graph_info': self.graph_ports,
                'storage_info': self.storage_ports,
            }

            json.dump(clusterinfo, fp)

    def cleanup(self):
        self.stop_graph()
        self.stop_storage()
        self.stop_meta()

    def start_storage(self, index=None):
        stors = [index] if index else range(self.storage_num)
        for x in stors:
            self._do_start_storage(x)

    def get_meta_server_addrs(self):
        addrs = ','.join(['{}:{}'.format(self.local_ip, port) for _, port in self.meta_ports.items()])
        return addrs

    def _do_start_storage(self, index):
        pid_file = os.path.join(self.pids_dir, 'nebula-storaged.pid.{}'.format(index))
        log_dir = os.path.join(self.logs_dir, 'storaged.{}'.format(index))
        port = self.storage_ports.get(str(index), None) or assign_port()
        meta_server_addr = self.get_meta_server_addrs()
        ws_http_port = assign_port()
        ws_h2_port = assign_port()
        data_path = os.path.join(self.data_dir, 'storaged.{}'.format(index))
        storage_conf_file = os.path.join(self.etc_dir, 'nebula-storaged.conf')

        print('starting storage {} with port {}'.format(index, port))
        cmd = ' '.join([os.path.join(self.bin_dir, 'nebula-storaged'),
            '--flagfile {}'.format(storage_conf_file),
            # '--local_config=false',
            '--pid_file {}'.format(pid_file),
            '--meta_server_addrs {}'.format(meta_server_addr),
            '--heartbeat_interval_secs {}'.format(1),
            '--raft_heartbeat_interval_secs {}'.format(1),
            '--minloglevel {}'.format(0),
            # '--num_worker_threads=1',
            # '--minloglevel {}'.format(0),
            '--log_dir {}'.format(log_dir),
            '--local_ip {}'.format(self.local_ip),
            '--trace_raft {}'.format('true'),
            '--port {}'.format(port),
            # '--trace_toss {}'.format('true'),
            # '-v {}'.format(2),
            '--ws_http_port {}'.format(ws_http_port),
            '--ws_h2_port {}'.format(ws_h2_port),
            '--data_path {}'.format(data_path)
        ])

        proc = subprocess.Popen(cmd, shell=True, cwd=self.cluster_dir)
        proc.wait()
        if check_port(port, wait=5, host=self.local_ip):
            self.storage_ports[str(index)] = port
        else:
            print('failed start storage listening at: {}'.format(port))
            raise Exception

    def stop_storage(self, index=None):
        stors = [index] if index else range(self.storage_num)
        for x in stors:
            print('stopping storage {}'.format(x))
            self._do_stop_storage(x)


    def _do_stop_storage(self, index):
        pid_file = os.path.join(self.pids_dir, 'nebula-storaged.pid.{}'.format(index))
        stop_process(pid_file)

    def start_graph(self, index=None):
        graphs = [index] if index else range(self.graph_num)
        for x in graphs:
            self._do_start_graph(x)

    def _do_start_graph(self, index):
        pid_file = os.path.join(self.pids_dir, 'nebula-graphd.pid.{}'.format(index))
        log_dir = os.path.join(self.logs_dir, 'graphd.{}'.format(index))
        meta_server_addrs = self.get_meta_server_addrs()
        # port = self.graph_ports.get(index, None) or assign_port()
        # port = 9669
        # TODO dirty hack, fix me later
        port = self.graph_ports.get(str(index), None) or assign_port()
        # import pdb; pdb.set_trace()
        print('graph using port {}'.format(port))
        # self.graph_port_list = self.graph_port_list[1:]
        ws_http_port = assign_port()
        ws_h2_port = assign_port()
        # TODO better local_ip assignment
        # ws_storage_http_port = assign_port()
        graph_conf_file = os.path.join(self.etc_dir, 'nebula-graphd.conf')
        self.graph_ports[str(index)] = port

        print('starting graph {} with port {}'.format(index, port))
        cmd = ' '.join([os.path.join(self.bin_dir, 'nebula-graphd'),
            '--flagfile {}'.format(graph_conf_file),
            '--meta_server_addrs {}'.format(meta_server_addrs),
            # '--local_config false',
            '--pid_file {}'.format(pid_file),
            '--log_dir {}'.format(log_dir),
            # '-v {}'.format(3),
            '--heartbeat_interval_secs {}'.format(1),
            '--enable_experimental_feature={}'.format('true'),
            '--local_ip {}'.format(self.local_ip),
            # '--minloglevel {}'.format(3),
            # '--minloglevel {}'.format(0),
            # '--num_netio_threads {}'.format(16),
            # '--num_worker_threads {}'.format(16),
            '--port {}'.format(port),
            '--ws_http_port {}'.format(ws_http_port),
            '--ws_h2_port {}'.format(ws_h2_port),
            # '--ws_storage_http_port {}'.format(ws_storage_http_port),
        ])

        proc = subprocess.Popen(cmd, shell=True, cwd=self.cluster_dir)
        proc.wait()

        if check_port(port, wait=5, host=self.local_ip):
            self.graph_ports[str(index)] = port
        else:
            print('failed start graph listening at: {}'.format(port))
            raise Exception

    def stop_graph(self, index=None):
        graphs = [index] if index else range(self.graph_num)
        for x in graphs:
            print('stopping graph {}'.format(x))
            self._do_stop_graph(x)

    def _do_stop_graph(self, index):
        pid_file = os.path.join(self.pids_dir, 'nebula-graphd.pid.{}'.format(index))
        stop_process(pid_file)

    def start_meta(self, index=None):
        metas = [index] if index else range(self.meta_num)
        for x in metas:
            port = self.meta_ports.get(str(x), None) or assign_port()
            # self.meta_ports[x] = port
            # import pdb; pdb.set_trace()
            self.meta_ports[str(x)] = port

        meta_server_addrs = self.get_meta_server_addrs()
        for x in metas:
            print('starting meta {}'.format(x))
            self._do_start_meta(x, self.meta_ports[str(x)], meta_server_addrs)

        for x in metas:
            port = self.meta_ports[str(x)]
            if not check_port(port, wait=5):
                print('failed start meta listening at: {}'.format(port))
                raise Exception

    def _do_start_meta(self, index, port, meta_server_addrs):
        pid_file = os.path.join(self.pids_dir, 'nebula-metad.pid.{}'.format(index))
        log_dir = os.path.join(self.logs_dir, 'metad.{}'.format(index))
        ws_http_port = assign_port()
        ws_h2_port = assign_port()
        # TODO better local_ip assignment
        # ws_storage_http_port = assign_port()
        data_path = os.path.join(self.data_dir, 'metad.{}'.format(index))
        meta_conf_file = os.path.join(self.etc_dir, 'nebula-metad.conf')

        cmd = ' '.join([os.path.join(self.bin_dir, 'nebula-metad'),
            '--flagfile {}'.format(meta_conf_file),
            # '--local_config false',
            '--pid_file {}'.format(pid_file),
            '--log_dir {}'.format(log_dir),
            '--local_ip {}'.format(self.local_ip),
            '--heartbeat_interval_secs {}'.format(1),
            '--raft_heartbeat_interval_secs {}'.format(1),
            '--port {}'.format(port),
            '--meta_server_addrs {}'.format(meta_server_addrs),
            '--ws_http_port {}'.format(ws_http_port),
            # '--minloglevel {}'.format(3),
            '--ws_h2_port {}'.format(ws_h2_port),
            # '--ws_storage_http_port {}'.format(ws_storage_http_port),
            '--data_path {}'.format(data_path)
        ])

        proc = subprocess.Popen(cmd, shell=True, cwd=self.cluster_dir)
        proc.wait()

    def stop_meta(self, index=None):
        metas = [index] if index else range(self.meta_num)
        for x in metas:
            print('stopping meta {}'.format(x))
            self._do_stop_meta(x)

    def _do_stop_meta(self, index):
        pid_file = os.path.join(self.pids_dir, 'nebula-metad.pid.{}'.format(index))
        stop_process(pid_file)
        print(self.meta_ports)

# if __name__ == '__main__':
#     build_dir = '/data/src/wwl/nebula/build'
#     cluster_dir = '/data/src/wwl/test'

#     mgr = NebulaManager(build_dir, cluster_dir=cluster_dir, storage_num=1, graph_num=1, meta_num=1, local_ip='192.168.15.11')
#     mgr.prepare()
#     mgr.cleanup()
#     mgr.cleanup()

