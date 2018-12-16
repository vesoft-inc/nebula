/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
DECLARE_bool(reuse_port);
DECLARE_int32(listen_backlog);

DECLARE_bool(redirect_stdout);
DECLARE_string(stdout_log_file);
DECLARE_string(stderr_log_file);


#endif  // GRAPH_GRAPHFLAGS_H_
