/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_AUTHENTICATOR_H_
#define SERVICE_AUTHENTICATOR_H_

#include "common/base/Base.h"

namespace nebula {
namespace graph {

class Authenticator {
public:
    virtual ~Authenticator() {}

    virtual bool MUST_USE_RESULT auth(const std::string &user,
                                      const std::string &password) = 0;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_AUTHENTICATOR_H_
