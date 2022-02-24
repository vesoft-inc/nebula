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

  // This interface is only implemented to override the parent class and should not be called
  Status auth(const std::string& user, const std::string& password) override;

  // TODO(Aiee) This is a walkround to address the problem that using a lower version(< v2.6.0)
  // client to connect with higher version(>= v3.0.0) Nebula service will cause a crash.
  //
  // clientIP here is used to check whether the client version is lower than v2.6.0
  Status auth(const std::string& user, const std::string& password, const HostAddr& clientIp);

 private:
  meta::MetaClient* metaClient_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PASSWORDAUTHENTICATOR_H_
