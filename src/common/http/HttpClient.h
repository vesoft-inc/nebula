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
    // Send a http GET request
    static StatusOr<std::string> get(const std::string& path, const std::string& options = "-G");

    // Send a http POST request
    static StatusOr<std::string> post(const std::string& path, const std::string& header);
    // Send a http POST request with a different function signature
    static StatusOr<std::string> post(const std::string& path,
                                      const std::unordered_map<std::string, std::string>& header);
    static StatusOr<std::string> post(const std::string& path,
                                      const folly::dynamic& data = folly::dynamic::object());

    // Send a http PUT request
    static StatusOr<std::string> put(const std::string& path, const std::string& header);
    static StatusOr<std::string> put(const std::string& path,
                                      const std::unordered_map<std::string, std::string>& header);
    static StatusOr<std::string> put(const std::string& path,
                                      const folly::dynamic& data = folly::dynamic::object());

protected:
    static StatusOr<std::string> sendRequest(const std::string& path,
                                             const folly::dynamic& data,
                                             const std::string& reqType);
};

}   // namespace http
}   // namespace nebula

#endif  // COMMON_HTTPCLIENT_H
