/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_CLOUDAUTHENTICATOR_H_
#define GRAPH_SERVICE_CLOUDAUTHENTICATOR_H_

#include <string>  // for string

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/Status.h"           // for Status
#include "graph/service/Authenticator.h"  // for Authenticator

namespace nebula {
namespace meta {
class MetaClient;

class MetaClient;
}  // namespace meta

namespace graph {

class CloudAuthenticator final : public Authenticator {
 public:
  explicit CloudAuthenticator(meta::MetaClient* client);

  Status auth(const std::string& user, const std::string& password) override;

 private:
  meta::MetaClient* metaClient_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CLOUDAUTHENTICATOR_H_
