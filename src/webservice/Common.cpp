/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "webservice/Common.h"

#include <gflags/gflags.h>

DEFINE_int32(ws_meta_http_port, 11000, "Port to listen on Meta with HTTP protocol");
DEFINE_int32(ws_meta_h2_port, 11002, "Port to listen on Meta with HTTP/2 protocol");
DEFINE_int32(ws_storage_http_port, 12000, "Port to listen on Storage with HTTP protocol");
DEFINE_int32(ws_storage_h2_port, 12002, "Port to listen on Storage with HTTP/2 protocol");

namespace nebula {

std::unordered_map<HttpStatusCode, std::string> WebServiceUtils::kStatusStringMap_ = {
    {HttpStatusCode::OK, "OK"},
    {HttpStatusCode::BAD_REQUEST, "Bad Request"},
    {HttpStatusCode::FORBIDDEN, "Forbidden"},
    {HttpStatusCode::NOT_FOUND, "Not Found"},
    {HttpStatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed"},
};

}  // namespace nebula
