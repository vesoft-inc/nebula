/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/PasswordAuthenticator.h"

namespace nebula {
namespace graph {

PasswordAuthenticator::PasswordAuthenticator(meta::MetaClient* client) {
  metaClient_ = client;
}

Status PasswordAuthenticator::auth(const std::string& user, const std::string& password) {
  return metaClient_->authCheckFromCache(user, password);
}

}  // namespace graph
}  // namespace nebula
