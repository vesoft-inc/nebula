/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_SIMPLEAUTHENTICATOR_H_
#define VGRAPHD_SIMPLEAUTHENTICATOR_H_

#include "base/Base.h"
#include "vgraphd/Authenticator.h"

namespace vesoft {
namespace vgraph {

class SimpleAuthenticator final : public Authenticator {
public:
    bool VE_MUST_USE_RESULT auth(const std::string &user,
                                 const std::string &password) override {
        if (user == "user" && password == "password") {
            return true;
        }
        return false;
    }

private:
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_SIMPLEAUTHENTICATOR_H_
