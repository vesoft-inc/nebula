/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaFlags.h"

DEFINE_int32(meta_port, 45500, "Nebula Meta daemon's listen port");
DEFINE_bool(meta_daemonize, true, "Whether run as a daemon process");
DEFINE_string(meta_data_path, "", "Root data path");
DEFINE_string(meta_peers, "", "It is a list of IPs split by comma,"
                              "the ips number equals replica number."
                              "If empty, it means replica is 1");
DEFINE_string(meta_local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_string(meta_pid_file, "pids/nebula-metad.pid", "File to hold the meta process id");
