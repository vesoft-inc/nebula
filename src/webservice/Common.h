/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WEBSERVICE_COMMON_H_
#define WEBSERVICE_COMMON_H_

#include <cstdint>
#include <string>
#include <unordered_map>

namespace nebula {

enum class HttpCode {
  SUCCEEDED = 0,
  E_UNSUPPORTED_METHOD = -1,
  E_UNPROCESSABLE = -2,
  E_ILLEGAL_ARGUMENT = -3,
};

enum class HttpStatusCode : int32_t {
  OK = 200,
  BAD_REQUEST = 400,
  FORBIDDEN = 403,
  NOT_FOUND = 404,
  METHOD_NOT_ALLOWED = 405,
};

class WebServiceUtils final {
 public:
  static int32_t to(HttpStatusCode code) { return static_cast<int32_t>(code); }
  static std::string toString(HttpStatusCode code) { return kStatusStringMap_[code]; }

 private:
  static std::unordered_map<HttpStatusCode, std::string> kStatusStringMap_;
};

}  // namespace nebula

#endif  // WEBSERVICE_COMMON_H_
