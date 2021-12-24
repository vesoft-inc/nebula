/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_PASSWORDAUTHENTICATOR_H_
#define GRAPH_SERVICE_PASSWORDAUTHENTICATOR_H_

#include "clients/meta/MetaClient.h"
#include "graph/service/Authenticator.h"

namespace nebula {
namespace graph {

class PasswordAuthenticator final : public Authenticator {
 public:
  explicit PasswordAuthenticator(meta::MetaClient* client);

  Status auth(const std::string& user, const std::string& password) override;

 private:
  meta::MetaClient* metaClient_;
  uint32 maxFailedLoginAttempts_ = 0;
  uint32 passwordLocalTime_ = 0;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PASSWORDAUTHENTICATOR_H_
