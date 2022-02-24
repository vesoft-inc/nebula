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
  UNUSED(user);
  UNUSED(password);
  return Status::Error(
      "Should not be called, this interface is implemented to override the parent class");
}

Status PasswordAuthenticator::auth(const std::string& user,
                                   const std::string& password,
                                   const HostAddr& clientIp) {
  return metaClient_->authCheckFromCache(user, password, clientIp);
}

}  // namespace graph
}  // namespace nebula
