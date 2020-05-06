/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CLOUDAUTHENTICATOR_H_
#define GRAPH_CLOUDAUTHENTICATOR_H_

#include "base/Base.h"
#include "graph/Authenticator.h"
#include "meta/client/MetaClient.h"

namespace nebula {
namespace graph {

class CloudAuthenticator final : public Authenticator {
public:
    explicit CloudAuthenticator(meta::MetaClient* client);

    bool auth(const std::string& user, const std::string& password) override;

private:
    meta::MetaClient*               metaClient_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CLOUDAUTHENTICATOR_H_
