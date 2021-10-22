/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SERVICE_AUTHENTICATOR_H_
#define GRAPH_SERVICE_AUTHENTICATOR_H_

#include "common/base/Base.h"

namespace nebula {
namespace graph {

class Authenticator {
 public:
  virtual ~Authenticator() {}

  virtual bool NG_MUST_USE_RESULT auth(const std::string &user, const std::string &password) = 0;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_AUTHENTICATOR_H_
