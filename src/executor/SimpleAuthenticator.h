/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SIMPLEAUTHENTICATOR_H_
#define GRAPH_SIMPLEAUTHENTICATOR_H_

#include "base/Base.h"
#include "graph/Authenticator.h"

namespace nebula {
namespace graph {

class SimpleAuthenticator final : public Authenticator {
public:
    bool MUST_USE_RESULT auth(const std::string &user,
                              const std::string &password) override {
        if (user == "user" && password == "password") {
            return true;
        }
        return false;
    }

private:
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_SIMPLEAUTHENTICATOR_H_
