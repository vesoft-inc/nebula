/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_AUTHENTICATOR_H_
#define GRAPH_AUTHENTICATOR_H_

#include "base/Base.h"

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
