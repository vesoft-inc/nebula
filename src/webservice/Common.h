/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef WEBSERVICE_COMMON_H_
#define WEBSERVICE_COMMON_H_

namespace nebula {

enum class HttpCode {
    SUCCEEDED            = 0,
    E_UNSUPPORTED_METHOD = -1,
    E_UNPROCESSABLE      = -2,
};

}  // namespace nebula
#endif  // WEBSERVICE_COMMON_H_
