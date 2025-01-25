/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphFlags.h"

#include "version/Version.h"

DEFINE_int32(port, 3699, "Graph service daemon's listen port");
DEFINE_int32(client_idle_timeout_secs,
             28800,
             "The number of seconds NebulaGraph service waits before closing the idle connections");
DEFINE_int32(session_idle_timeout_secs, 28800, "The number of seconds before idle sessions expire");
DEFINE_int32(
    session_reclaim_interval_secs,
    60,
    "Period we try to reclaim expired sessions and update session information to the meta server");
DEFINE_int32(num_netio_threads,
             0,
             "The number of networking threads, 0 for number of physical CPU cores");
DEFINE_int32(num_accept_threads, 1, "Number of threads to accept incoming connections");
DEFINE_uint32(num_max_connections,
              0,
              "Max active connections for all networking threads. 0 means no limit. Max active "
              "connections for each networking thread = num_max_connections / num_netio_threads");
DEFINE_int32(num_worker_threads, 0, "Number of threads to execute user queries");
DEFINE_int32(num_operator_threads, 2, "Number of threads to execute a single operator");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_int32(listen_backlog, 1024, "Backlog of the listen socket");
DEFINE_string(listen_netdev, "any", "The network device to listen on");
DEFINE_string(local_ip, "", "Local ip specified for graphd");
DEFINE_string(pid_file, "pids/nebula-graphd.pid", "File to hold the process id");

DEFINE_bool(daemonize, true, "Whether run as a daemon process");
DEFINE_string(meta_server_addrs,
              "",
              "list of meta server addresses,"
              "the format looks like ip1:port1, ip2:port2, ip3:port3");
DEFINE_bool(local_config, true, "meta client will not retrieve latest configuration from meta");

DEFINE_string(default_charset, "utf8", "The default charset when a space is created");
DEFINE_string(default_collate, "utf8_bin", "The default collate when a space is created");

DEFINE_bool(enable_authorize, false, "Enable authorization, default false");
DEFINE_string(auth_type,
              "password",
              "User login authentication type,"
              "password for nebula authentication,"
              "ldap for ldap authentication,"
              "cloud for cloud authentication");

DEFINE_string(cloud_http_url, "", "cloud http url including ip, port, url path");
DEFINE_uint32(max_allowed_statements, 512, "Max allowed sequential statements");
DEFINE_uint32(max_allowed_query_size, 4194304, "Max allowed sequential query size");

DEFINE_uint32(max_statements,
              1024,
              "threshold for maximun number of statements that can be validate");
DEFINE_int64(max_allowed_connections,
             std::numeric_limits<int64_t>::max(),
             "Max connections of the whole cluster");

DEFINE_bool(enable_optimizer, false, "Whether to enable optimizer");

#ifndef BUILD_STANDALONE
DEFINE_uint32(ft_request_retry_times, 3, "Retry times if fulltext request failed");
// TODO(weygu): revisit this conf for vector
DEFINE_bool(enable_client_white_list, true, "Turn on/off the client white list.");
DEFINE_string(client_white_list,
              nebula::getOriginVersion() + ":3.0.0",
              "A white list for different client handshakeKey, separate with colon.");

#endif

DEFINE_bool(accept_partial_success, false, "Whether to accept partial success, default false");

DEFINE_bool(disable_octal_escape_char,
            false,
            "Octal escape character will be disabled"
            " in next version to ensure compatibility with cypher.");

DEFINE_bool(enable_experimental_feature, false, "Whether to enable experimental feature");
DEFINE_bool(enable_data_balance, true, "Whether to enable data balance feature");

DEFINE_int32(num_rows_to_check_memory, 1024, "number rows to check memory");
DEFINE_int32(max_sessions_per_ip_per_user,
             300,
             "Maximum number of sessions that can be created per IP and per user");

DEFINE_bool(optimize_appendvertices, false, "if true, return directly without go through RPC");

DEFINE_uint32(num_path_thread, 10, "number of threads to build path");

// Sanity-checking Flag Values
static bool ValidateSessIdleTimeout(const char* flagname, int32_t value) {
  // The max timeout is 604800 seconds(a week)
  if (value > 0 && value < 604801)  // value is ok
    return true;

  FLOG_WARN("Invalid value for --%s: %d, the timeout should be an integer between 1 and 604800\n",
            flagname,
            (int)value);
  return false;
}
DEFINE_validator(session_idle_timeout_secs, &ValidateSessIdleTimeout);
DEFINE_validator(client_idle_timeout_secs, &ValidateSessIdleTimeout);

DEFINE_int32(min_batch_size,
             8192,
             "The min batch size for handling dataset in multi job mode, only enabled when "
             "max_job_size is greater than 1.");
DEFINE_int32(max_job_size, 1, "The max job size in multi job mode.");

DEFINE_bool(enable_async_gc, false, "If enable async gc.");
DEFINE_uint32(
    gc_worker_size,
    0,
    "Background garbage clean workers, default number is 0 which means using hardware core size.");

DEFINE_bool(graph_use_vertex_key, false, "whether allow insert or query the vertex key");
