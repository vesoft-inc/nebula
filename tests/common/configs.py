# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

all_configs = {'--address'        : ['address', '', 'Address of the Nebula'],
               '--user'           : ['user', 'user', 'The user of Nebula'],
               '--password'       : ['password', 'password', 'The password of Nebula'],
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
    resp = client.execute_query(
        'get configs GRAPH:heartbeat_interval_secs')
    assert resp.error_code == 0
    assert len(resp.data.rows) == 1, "invalid row size: {}".format(resp.data.rows)
    graph_delay = resp.data.rows[0].values[4].get_iVal() + 1

    resp = client.execute_query(
        'get configs STORAGE:heartbeat_interval_secs')
    assert resp.error_code == 0
    assert len(resp.data.rows) == 1, "invalid row size: {}".format(resp.data.rows)
    storage_delay = resp.data.rows[0].values[4].get_iVal() + 1
    delay = max(graph_delay, storage_delay) * 3
    return delay
