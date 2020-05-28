/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_HTTPCLIENT_H
#define COMMON_HTTPCLIENT_H

#include "common/base/Base.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace http {

class HttpClient {
public:
    HttpClient() = delete;

    ~HttpClient() = default;

    static StatusOr<std::string> get(const std::string& path, const std::string& options = "-G");
};

}   // namespace http
}   // namespace nebula

#endif  // COMMON_HTTPCLIENT_H
