/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_CLOUDAUTHENTICATOR_H_
#define SERVICE_CLOUDAUTHENTICATOR_H_

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"
#include "service/Authenticator.h"

namespace nebula {
namespace graph {

class CloudAuthenticator final : public Authenticator {
public:
    explicit CloudAuthenticator(const meta::MetaClient* client);

    bool auth(const std::string& user, const std::string& password) override;

private:
    const meta::MetaClient*               metaClient_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CLOUDAUTHENTICATOR_H_
