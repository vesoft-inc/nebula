# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os

DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data",
)

all_configs = {'--address'        : ['address', '', 'Address of the Nebula'],
               '--user'           : ['user', 'root', 'The user of Nebula'],
               '--password'       : ['password', 'nebula', 'The password of Nebula'],
               '--partition_num'  : ['partition_num', '10', 'The partition_num of Nebula\'s space'],
               '--replica_factor' : ['replica_factor', '1', 'The replica_factor of Nebula\'s space'],
               '--data_dir'       : ['data_dir', '', 'Data Preload Directory for Nebula'],
               '--stop_nebula'    : ['stop_nebula', 'true', 'Stop the nebula services'],
               '--rm_dir'         : ['rm_dir', 'true', 'Remove the temp test dir'],
               '--debug_log'      : ['debug_log', 'true', 'Set nebula service --v=4']}


def init_configs():
    from optparse import OptionParser
    opt_parser = OptionParser()
    for config in all_configs:
        opt_parser.add_option(config,
                              dest = all_configs[config][0],
                              default = all_configs[config][1],
                              help = all_configs[config][2])

    # the configs only for pytest
    opt_parser.add_option('-n',
                          dest = 'n',
                          default = 'auto',
                          help = 'pytest use it')

    opt_parser.add_option('--dist',
                          dest = 'dist',
                          default = 'loadfile',
                          help = 'pytest use it')
    return opt_parser


def get_delay_time(client):
    resp = client.execute(
        'get configs GRAPH:heartbeat_interval_secs')
    assert resp.is_succeeded()
    assert resp.row_size() == 1, "invalid row size: {}".format(resp.rows())
    graph_delay = resp.row_values(0)[4].as_int() + 1

    resp = client.execute(
        'get configs STORAGE:heartbeat_interval_secs')
    assert resp.is_succeeded()
    assert resp.row_size() == 1, "invalid row size: {}".format(resp.rows())
    storage_delay = resp.row_values(0)[4].as_int() + 1
    delay = max(graph_delay, storage_delay) * 3
    return delay
