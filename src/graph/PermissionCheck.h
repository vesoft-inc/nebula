/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/permission/PermissionManager.h"
#include "parser/Sentence.h"
#include "parser/TraverseSentences.h"
#include "parser/AdminSentences.h"

#ifndef GRAPH_PERMISSIONCHECK_H_
#define GRAPH_PERMISSIONCHECK_H_

namespace nebula {
namespace graph {

class PermissionCheck final {
public:
    PermissionCheck() = delete;

    static bool permissionCheck(session::Session *session, Sentence* sentence);
};
}  // namespace graph
}  // namespace nebula

#endif   // GRAPH_PERMISSIONCHECK_H_
