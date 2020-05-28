/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_WEBSERVICE_COMMON_H_
#define COMMON_WEBSERVICE_COMMON_H_

#include "common/base/Base.h"

DECLARE_int32(ws_meta_http_port);
DECLARE_int32(ws_meta_h2_port);
DECLARE_int32(ws_storage_http_port);
DECLARE_int32(ws_storage_h2_port);

namespace nebula {

enum class HttpCode {
    SUCCEEDED            = 0,
    E_UNSUPPORTED_METHOD = -1,
    E_UNPROCESSABLE      = -2,
    E_ILLEGAL_ARGUMENT   = -3,
};

enum class HttpStatusCode {
    OK                   = 200,
    BAD_REQUEST          = 400,
    FORBIDDEN            = 403,
    NOT_FOUND            = 404,
    METHOD_NOT_ALLOWED   = 405,
};

static std::map<HttpStatusCode, std::string> statusStringMap {
    {HttpStatusCode::OK,                     "OK"},
    {HttpStatusCode::BAD_REQUEST,            "Bad Request"},
    {HttpStatusCode::FORBIDDEN,              "Forbidden"},
    {HttpStatusCode::NOT_FOUND,              "Not Found"},
    {HttpStatusCode::METHOD_NOT_ALLOWED,     "Method Not Allowed"}
};

class WebServiceUtils final {
public:
    static int32_t to(HttpStatusCode code) {
        return static_cast<int32_t>(code);
    }

    static std::string toString(HttpStatusCode code) {
        return statusStringMap[code];
    }
};

}  // namespace nebula
#endif  // COMMON_WEBSERVICE_COMMON_H_
