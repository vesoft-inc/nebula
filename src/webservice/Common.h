/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WEBSERVICE_COMMON_H_
#define WEBSERVICE_COMMON_H_

namespace nebula {

enum class HttpCode {
    SUCCEEDED            = 0,
    E_UNSUPPORTED_METHOD = -1,
    E_UNPROCESSABLE      = -2,
    E_ILLEGAL_ARGUMENT   = -3,
};

}  // namespace nebula
#endif  // WEBSERVICE_COMMON_H_
