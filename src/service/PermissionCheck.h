/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/TraverseSentences.h"
#include "parser/AdminSentences.h"
#include "service/PermissionManager.h"

#ifndef SERVICE_PERMISSIONCHECK_H_
#define SERVICE_PERMISSIONCHECK_H_

namespace nebula {
namespace graph {

class PermissionCheck final {
public:
    PermissionCheck() = delete;

    static Status permissionCheck(ClientSession *session,
                                  Sentence* sentence,
                                  ValidateContext *vctx,
                                  GraphSpaceID targetSpace = -1);
};
}  // namespace graph
}  // namespace nebula

#endif   // GRAPH_PERMISSIONCHECK_H_
