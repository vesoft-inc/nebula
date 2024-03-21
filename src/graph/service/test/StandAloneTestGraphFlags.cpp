/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphFlags.h"
#include "version/Version.h"

DEFINE_uint32(ft_request_retry_times, 3, "Retry times if fulltext request failed");
DEFINE_bool(enable_client_white_list, true, "Turn on/off the client white list.");
DEFINE_string(client_white_list,
              nebula::getOriginVersion() + ":3.0.0",
              "A white list for different client handshakeKey, separate with colon.");
