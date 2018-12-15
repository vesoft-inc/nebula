/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_AUTHENTICATOR_H_
#define VGRAPHD_AUTHENTICATOR_H_

#include "base/Base.h"

namespace vesoft {
namespace vgraph {

class Authenticator {
public:
    virtual ~Authenticator() {};

    virtual bool VE_MUST_USE_RESULT auth(const std::string &user,
                                         const std::string &password) = 0;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_AUTHENTICATOR_H_
