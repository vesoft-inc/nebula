/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef GRAPH_PERMISSIONMANAGER_H
#define GRAPH_PERMISSIONMANAGER_H

// Operation and permission define:
// Operation        | GOD           | ADMIN         | USER          | GUEST
// ---------------- | ------------- | ------------- | ------------- | -------------
// kGo              | Y             | Y             | Y             | Y
// kSet             | Y             | Y             | Y             | Y
// kPipe            | Y             | Y             | Y             | Y
// kUse             | Y             | Y             | Y             | Y
// kMatch           | Y             | Y             | Y             | Y
// kAssignment      | Y             | Y             | Y             | Y
// kCreateTag       | Y             | Y             |               |
// kAlterTag        | Y             | Y             |               |
// kCreateEdge      | Y             | Y             |               |
// kAlterEdge       | Y             | Y             |               |
// kDescribeTag     | Y             | Y             | Y             | Y
// kDescribeEdge    | Y             | Y             | Y             | Y
// kRemoveTag       | Y             | Y             |               |
// kRemoveEdge      | Y             | Y             |               |
// kInsertVertex    | Y             | Y             | Y             |
// kInsertEdge      | Y             | Y             | Y             |
// kShow            | Y             | Y             | Y             | Y
// kDeleteVertex    | Y             | Y             | Y             |
// kDeleteEdge      | Y             | Y             | Y             |
// kFind            | Y             | Y             | Y             | Y
// kCreateSpace     | Y             |               |               |
// kDropSpace       | Y             | Y             |               |
// kYield           | Y             | Y             | Y             | Y
// kCreateUser      | Y             |               |               |
// kDropUser        | Y             |               |               |
// kAlterUser       | Y             | Y             | Y             | Y
// kGrant           | Y             | Y             |               |
// kRevoke          | Y             | Y             |               |
// kChangePassword  | Y             | Y             | Y             | Y


#endif  // GRAPH_PERMISSIONMANAGER_H
