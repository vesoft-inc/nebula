/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SERVICE_GRAPHFLAGS_H_
#define GRAPH_SERVICE_GRAPHFLAGS_H_

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
DECLARE_bool(disable_octal_escape_char);

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

// optimizer
DECLARE_bool(enable_optimizer);

DECLARE_int64(max_allowed_connections);

DECLARE_string(local_ip);

DECLARE_bool(enable_experimental_feature);

DECLARE_bool(enable_client_white_list);
DECLARE_string(client_white_list);

DECLARE_int32(num_rows_to_check_memory);

#endif  // GRAPH_GRAPHFLAGS_H_
