/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SERVICE_PASSWORDAUTHENTICATOR_H_
#define GRAPH_SERVICE_PASSWORDAUTHENTICATOR_H_

#include "clients/meta/MetaClient.h"
#include "graph/service/Authenticator.h"

namespace nebula {
namespace graph {

class PasswordAuthenticator final : public Authenticator {
 public:
  explicit PasswordAuthenticator(const meta::MetaClient* client);

  bool auth(const std::string& user, const std::string& password) override;

 private:
  const meta::MetaClient* metaClient_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PASSWORDAUTHENTICATOR_H_
