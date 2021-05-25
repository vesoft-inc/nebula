/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_GRAPHFLAGS_H_
#define SERVICE_GRAPHFLAGS_H_

#include "common/base/Base.h"

DECLARE_int32(port);
DECLARE_int32(client_idle_timeout_secs);
DECLARE_int32(session_idle_timeout_secs);
DECLARE_int32(session_reclaim_interval_secs);
DECLARE_int32(num_netio_threads);
DECLARE_int32(num_accept_threads);
DECLARE_int32(num_worker_threads);
DECLARE_bool(reuse_port);
DECLARE_int32(listen_backlog);
DECLARE_string(listen_netdev);
DECLARE_string(local_ip);
DECLARE_string(pid_file);
DECLARE_bool(local_config);
DECLARE_bool(accept_partial_success);

DECLARE_bool(redirect_stdout);
DECLARE_string(stdout_log_file);
DECLARE_string(stderr_log_file);
DECLARE_bool(daemonize);
DECLARE_string(meta_server_addrs);

DECLARE_string(default_charset);
DECLARE_string(default_collate);
DECLARE_bool(enable_authorize);
DECLARE_string(auth_type);
DECLARE_string(cloud_http_url);
DECLARE_uint32(max_allowed_statements);
DECLARE_double(system_memory_high_watermark_ratio);

// optimizer
DECLARE_bool(enable_optimizer);

#endif   // GRAPH_GRAPHFLAGS_H_
