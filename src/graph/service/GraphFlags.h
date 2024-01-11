/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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
DECLARE_uint32(num_max_connections);
DECLARE_int32(num_worker_threads);
DECLARE_int32(num_operator_threads);
DECLARE_bool(reuse_port);
DECLARE_int32(listen_backlog);
DECLARE_string(listen_netdev);
DECLARE_string(local_ip);
DECLARE_string(pid_file);
DECLARE_bool(enable_udf);
DECLARE_string(udf_path);
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
DECLARE_int32(max_sessions_per_ip_per_user);

DECLARE_uint32(max_statements);
// Failed login attempt
// value of failed_login_attempts is in the range from 0 to 32767.
// The deault value is 0. A value of 0 disables the option.
DECLARE_uint32(failed_login_attempts);
// value of password_lock_time_in_secs is in the range from 0 to 32767[secs].
// The deault value is 0. A value of 0 disables the option.
DECLARE_uint32(password_lock_time_in_secs);

// Optimizer
DECLARE_bool(enable_optimizer);
DECLARE_bool(optimize_appendvertice);
DECLARE_uint32(num_path_thread);

DECLARE_int64(max_allowed_connections);

DECLARE_bool(enable_experimental_feature);
DECLARE_bool(enable_data_balance);

DECLARE_bool(enable_client_white_list);
DECLARE_string(handshakeKey);
DECLARE_string(client_white_list);

DECLARE_int32(num_rows_to_check_memory);

DECLARE_int32(min_batch_size);
DECLARE_int32(max_job_size);

DECLARE_bool(enable_async_gc);
DECLARE_uint32(gc_worker_size);

DECLARE_bool(graph_use_vertex_key);

#endif  // GRAPH_GRAPHFLAGS_H_
