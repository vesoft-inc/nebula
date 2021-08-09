/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/PasswordAuthenticator.h"

namespace nebula {
namespace graph {

PasswordAuthenticator::PasswordAuthenticator(const meta::MetaClient* client) {
    metaClient_ = client;
}

bool PasswordAuthenticator::auth(const std::string& user, const std::string& password) {
    return metaClient_->authCheckFromCache(user, password);
}

}   // namespace graph
}   // namespace nebula
