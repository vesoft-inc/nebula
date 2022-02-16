/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_PASSWORDAUTHENTICATOR_H_
#define GRAPH_SERVICE_PASSWORDAUTHENTICATOR_H_

#include <string>  // for string

#include "clients/meta/MetaClient.h"
#include "common/base/Status.h"           // for Status
#include "graph/service/Authenticator.h"  // for Authenticator

namespace nebula {
namespace meta {
class MetaClient;

class MetaClient;
}  // namespace meta

namespace graph {

class PasswordAuthenticator final : public Authenticator {
 public:
  explicit PasswordAuthenticator(meta::MetaClient* client);

  Status auth(const std::string& user, const std::string& password) override;

 private:
  meta::MetaClient* metaClient_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PASSWORDAUTHENTICATOR_H_
