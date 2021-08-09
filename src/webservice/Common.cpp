/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/webservice/Common.h"

DEFINE_int32(ws_meta_http_port, 11000, "Port to listen on Meta with HTTP protocol");
DEFINE_int32(ws_meta_h2_port, 11002, "Port to listen on Meta with HTTP/2 protocol");
DEFINE_int32(ws_storage_http_port, 12000, "Port to listen on Storage with HTTP protocol");
DEFINE_int32(ws_storage_h2_port, 12002, "Port to listen on Storage with HTTP/2 protocol");
