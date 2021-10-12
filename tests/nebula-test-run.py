#!/usr/bin/env python3
# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import json
import os
import shutil
from tests.common.nebula_service import NebulaService
from tests.common.utils import get_conn_pool, load_csv_data
from tests.common.constants import NEBULA_HOME, TMP_DIR, NB_TMP_PATH, SPACE_TMP_PATH


CURR_PATH = os.path.dirname(os.path.abspath(__file__))


def init_parser():
    from optparse import OptionParser
    opt_parser = OptionParser()
    opt_parser.add_option('--build_dir',
                          dest='build_dir',
                          default=os.path.join(NEBULA_HOME, 'build'),
                          help='Build directory of nebula graph')
    opt_parser.add_option('--rm_dir',
                          dest='rm_dir',
                          default='true',
                          help='Whether to remove the test folder')
    opt_parser.add_option('--user',
                          dest='user',
                          default='root',
                          help='nebula graph user')
    opt_parser.add_option('--password',
                          dest='password',
                          default='nebula',
                          help='nebula graph password')
    opt_parser.add_option('--cmd',
                          dest='cmd',
                          default='',
                          help='start or stop command')
    opt_parser.add_option('--multi_graphd',
                          dest='multi_graphd',
                          default='',
                          help='Support multi graphds')
    opt_parser.add_option('--address',
                          dest='address',
                          default='',
                          help='Address of the Nebula')
    opt_parser.add_option('--debug',
                          dest='debug',
                          default=True,
                          help='Print verbose debug logs')
    opt_parser.add_option('--enable_ssl',
                          dest='enable_ssl',
                          default='false',
                          help='Whether enable SSL for cluster.')
    opt_parser.add_option('--enable_graph_ssl',
                          dest='enable_graph_ssl',
                          default='false',
                          help='Whether enable SSL for graph server.')
    opt_parser.add_option('--enable_meta_ssl',
                          dest='enable_meta_ssl',
                          default='false',
                          help='Whether enable SSL for meta server.')
    opt_parser.add_option('--ca_signed',
                          dest='ca_signed',
                          default='false',
                          help='Whether enable CA signed SSL/TLS mode.')
    opt_parser.add_option('--containerized',
                          dest='containerized',
                          default='false',
                          help='run this process inside container')
    return opt_parser


def opt_is(val, expect):
    return type(val) == str and val.lower() == expect


def start_nebula(nb, configs):
    if configs.address is not None and configs.address != "":
        print('test remote nebula graph, address is {}'.format(configs.address))
        if len(configs.address.split(':')) != 2:
            raise Exception('Invalid address, address is {}'.format(configs.address))
        address, port = configs.address.split(':')
        ports = [int(port)]
    else:
        nb.install()
        address = "localhost"
        ports = nb.start(
            debug_log=opt_is(configs.debug, "true"),
            multi_graphd=configs.multi_graphd,
            ca_signed=opt_is(configs.ca_signed, "true"),
            enable_ssl=opt_is(configs.enable_ssl, "true"),
            enable_graph_ssl=opt_is(configs.enable_graph_ssl, "true"),
            enable_meta_ssl=opt_is(configs.enable_meta_ssl, "true"),
            containerized=opt_is(configs.containerized, "true")
        )

    # Load csv data
    pool = get_conn_pool(address, ports[0])
    sess = pool.get_session(configs.user, configs.password)

    if not os.path.exists(TMP_DIR):
        os.mkdir(TMP_DIR)

    with open(SPACE_TMP_PATH, "w") as f:
        spaces = []
        folder = os.path.join(CURR_PATH, "data")
        for space in os.listdir(folder):
            if not os.path.exists(os.path.join(folder, space, "config.yaml")):
                continue
            data_dir = os.path.join(folder, space)
            space_desc = load_csv_data(sess, data_dir, space)
            spaces.append(space_desc.__dict__)
        f.write(json.dumps(spaces))

    with open(NB_TMP_PATH, "w") as f:
        data = {
            "ip": "localhost",
            "port": ports,
            "work_dir": nb.work_dir
        }
        f.write(json.dumps(data))
    print('Start nebula successfully')


def stop_nebula(nb, configs=None):
    if configs.address is not None and configs.address != "":
        print('test remote nebula graph, no need to stop nebula.')
        return

    with open(NB_TMP_PATH, "r") as f:
        data = json.loads(f.readline())
        nb.set_work_dir(data["work_dir"])

    cleanup = opt_is(configs.rm_dir, "true")
    nb.stop(cleanup)

    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print('nebula services have been stopped.')


if __name__ == "__main__":
    try:
        parser = init_parser()
        (configs, opts) = parser.parse_args()

        # Setup nebula graph service
        nebula_svc = NebulaService(configs.build_dir, NEBULA_HOME)

        if opt_is(configs.cmd, "start"):
            start_nebula(nebula_svc, configs)
        elif opt_is(configs.cmd, "stop"):
            stop_nebula(nebula_svc, configs)
        else:
            raise ValueError(f"Invalid parser args: {configs.cmd}")
    except Exception as x:
        print('\033[31m' + str(x) + '\033[0m')
        import traceback
        print(traceback.format_exc())
