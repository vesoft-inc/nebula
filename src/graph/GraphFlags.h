/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GRAPHFLAGS_H_
#define GRAPH_GRAPHFLAGS_H_

#include "base/Base.h"

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
DECLARE_string(pid_file);

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

// LDAP authentication common parameters
DECLARE_string(ldap_server);
DECLARE_int32(ldap_port);
DECLARE_string(ldap_scheme);
DECLARE_bool(ldap_tls);

// LDAP authentication simple bind mode parameters
DECLARE_string(ldap_prefix);
DECLARE_string(ldap_suffix);

// LDAP authentication search bind mode parameters
DECLARE_string(ldap_basedn);
DECLARE_string(ldap_binddn);
DECLARE_string(ldap_bindpasswd);
DECLARE_string(ldap_searchattribute);
DECLARE_string(ldap_searchfilter);

#endif  // GRAPH_GRAPHFLAGS_H_
