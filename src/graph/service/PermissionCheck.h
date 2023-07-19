/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphFlags.h"
#include "graph/service/PermissionManager.h"
#include "parser/AdminSentences.h"
#include "parser/TraverseSentences.h"

#ifndef GRAPH_SERVICE_PERMISSIONCHECK_H_
#define GRAPH_SERVICE_PERMISSIONCHECK_H_

namespace nebula {
namespace graph {

class PermissionCheck final {
 public:
  PermissionCheck() = delete;

  static Status permissionCheck(ClientSession *session,
                                Sentence *sentence,
                                ValidateContext *vctx,
                                GraphSpaceID targetSpace = -1);
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PERMISSIONCHECK_H_
