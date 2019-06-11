/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SIMPLEAUTHENTICATOR_H_
#define GRAPH_SIMPLEAUTHENTICATOR_H_

#include "base/Base.h"
#include "base/MD5Utils.h"
#include "graph/Authenticator.h"
#include "graph/GraphFlags.h"

namespace nebula {
namespace graph {

class SimpleAuthenticator final : public Authenticator {
public:
    explicit SimpleAuthenticator(meta::MetaClient *metaClient) {
        metaClient_ = metaClient;
    }
    bool MUST_USE_RESULT auth(const std::string &user,
                              const std::string &password) override {
        if (!FLAGS_security_authorization_enable) {
            return true;
        }
        auto ret = metaClient_->checkPassword(user, MD5Utils::md5Encode(password)).get();
        return ret.ok();
    }

private:
    meta::MetaClient                           *metaClient_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_SIMPLEAUTHENTICATOR_H_
