/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GraphFlags.h"

DEFINE_int32(port, 3699, "Nebula Graph daemon's listen port");
DEFINE_int32(client_idle_timeout_secs, 0,
                "Seconds before we close the idle connections, 0 for infinite");
DEFINE_int32(session_idle_timeout_secs, 600,
                "Seconds before we expire the idle sessions, 0 for infinite");
DEFINE_int32(session_reclaim_interval_secs, 10, "Period we try to reclaim expired sessions");
DEFINE_int32(num_netio_threads, 0,
                "Number of networking threads, 0 for number of physical CPU cores");
DEFINE_int32(num_accept_threads, 1, "Number of threads to accept incoming connections");
DEFINE_int32(num_worker_threads, 0, "Number of threads to execute user queries");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_int32(listen_backlog, 1024, "Backlog of the listen socket");
DEFINE_string(listen_netdev, "any", "The network device to listen on");
DEFINE_string(pid_file, "pids/nebula-graphd.pid", "File to hold the process id");

DEFINE_bool(redirect_stdout, true, "Whether to redirect stdout and stderr to separate files");
DEFINE_string(stdout_log_file, "graphd-stdout.log", "Destination filename of stdout");
DEFINE_string(stderr_log_file, "graphd-stderr.log", "Destination filename of stderr");
DEFINE_bool(daemonize, true, "Whether run as a daemon process");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");
