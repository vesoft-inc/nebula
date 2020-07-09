/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/PermissionCheck.h"

namespace nebula {
namespace graph {

/**
 * Read space : kUse, kDescribeSpace
 * Write space : kCreateSpace, kDropSpace, kCreateSnapshot, kDropSnapshot
 *               kBalance, kAdmin, kConfig, kIngest, kDownload
 * Read schema : kDescribeTag, kDescribeEdge,
 *               kDescribeTagIndex, kDescribeEdgeIndex
 * Write schema : kCreateTag, kAlterTag, kCreateEdge,
 *                kAlterEdge, kDropTag, kDropEdge,
 *                kCreateTagIndex, kCreateEdgeIndex, kDropTagIndex, kDropEdgeIndex,
 * Read user :
 * Write user : kCreateUser, kDropUser, kAlterUser,
 * Write role : kGrant, kRevoke,
 * Read data : kGo , kSet, kPipe, kMatch, kAssignment, kLookup,
 *             kYield, kOrderBy, kFetchVertices, kFind
 *             kFetchEdges, kFindPath, kLimit, KGroupBy, kReturn
 * Write data: kBuildTagIndex, kBuildEdgeIndex,
 *             kInsertVertex, kUpdateVertex, kInsertEdge,
 *             kUpdateEdge, kDeleteVertex, kDeleteEdges
 * Special operation : kShow, kChangePassword
 */

// static
bool PermissionCheck::permissionCheck(Session *session,
                                      Sentence* sentence,
                                      GraphSpaceID targetSpace) {
    if (!FLAGS_enable_authorize) {
        return true;
    }
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kUnknown : {
            return false;
        }
        case Sentence::Kind::kUse :
        case Sentence::Kind::kDescribeSpace : {
            /**
             * Use space and Describe space are special operations.
             * Permission checking needs to be done in their executor.
             * skip the check at here.
             */
            return true;
        }
        case Sentence::Kind::kCreateSpace :
        case Sentence::Kind::kDropSpace :
        case Sentence::Kind::kCreateSnapshot :
        case Sentence::Kind::kDropSnapshot :
        case Sentence::Kind::kBalance :
        case Sentence::Kind::kAdmin :
        case Sentence::Kind::kIngest :
        case Sentence::Kind::kConfig :
        case Sentence::Kind::kDownload : {
            return PermissionManager::canWriteSpace(session);
        }
        case Sentence::Kind::kCreateTag :
        case Sentence::Kind::kAlterTag :
        case Sentence::Kind::kCreateEdge :
        case Sentence::Kind::kAlterEdge :
        case Sentence::Kind::kDropTag :
        case Sentence::Kind::kDropEdge :
        case Sentence::Kind::kCreateTagIndex :
        case Sentence::Kind::kCreateEdgeIndex :
        case Sentence::Kind::kDropTagIndex :
        case Sentence::Kind::kDropEdgeIndex : {
            return PermissionManager::canWriteSchema(session);
        }
        case Sentence::Kind::kCreateUser :
        case Sentence::Kind::kDropUser :
        case Sentence::Kind::kAlterUser : {
            return PermissionManager::canWriteUser(session);
        }
        case Sentence::Kind::kRevoke :
        case Sentence::Kind::kGrant : {
            /**
             * Use grant and revoke are special operations.
             * Because have not found the target space id and target role
             * so permission checking needs to be done in their executor.
             * skip the check at here.
             */
            return true;
        }
        case Sentence::Kind::kRebuildTagIndex :
        case Sentence::Kind::kRebuildEdgeIndex :
        case Sentence::Kind::kInsertVertices :
        case Sentence::Kind::kUpdateVertex :
        case Sentence::Kind::kInsertEdges :
        case Sentence::Kind::kUpdateEdge :
        case Sentence::Kind::kDeleteVertex :
        case Sentence::Kind::kDeleteEdges : {
            return PermissionManager::canWriteData(session);
        }
        case Sentence::Kind::kDescribeTag :
        case Sentence::Kind::kDescribeEdge :
        case Sentence::Kind::kDescribeTagIndex :
        case Sentence::Kind::kDescribeEdgeIndex :
        case Sentence::Kind::kGo :
        case Sentence::Kind::kSet :
        case Sentence::Kind::kPipe :
        case Sentence::Kind::kMatch :
        case Sentence::Kind::kAssignment :
        case Sentence::Kind::kLookup :
        case Sentence::Kind::kYield :
        case Sentence::Kind::kOrderBy :
        case Sentence::Kind::kFetchVertices :
        case Sentence::Kind::kFetchEdges :
        case Sentence::Kind::kFindPath :
        case Sentence::Kind::kGetSubgraph:
        case Sentence::Kind::kLimit :
        case Sentence::Kind::KGroupBy :
        case Sentence::Kind::kReturn : {
            return PermissionManager::canReadSchemaOrData(session);
        }
        case Sentence::Kind::kShowParts:
        case Sentence::Kind::kShowTags:
        case Sentence::Kind::kShowEdges:
        case Sentence::Kind::kShowTagIndexes:
        case Sentence::Kind::kShowEdgeIndexes:
        case Sentence::Kind::kShowCreateTag:
        case Sentence::Kind::kShowCreateEdge:
        case Sentence::Kind::kShowCreateTagIndex:
        case Sentence::Kind::kShowCreateEdgeIndex:
        case Sentence::Kind::kShowTagIndexStatus:
        case Sentence::Kind::kShowEdgeIndexStatus: {
            /**
             * Above operations can get the space id via session,
             * so the permission same with canReadSchemaOrData.
             * They've been checked by "USE SPACE", so here skip the check.
             */
            return true;
        }
        case Sentence::Kind::kShowCharset:
        case Sentence::Kind::kShowCollation:
        case Sentence::Kind::kShowHosts: {
            /**
             * all roles can be show for above operations.
             */
            return true;
        }
        case Sentence::Kind::kShowSpaces:
        case Sentence::Kind::kShowCreateSpace:
        case Sentence::Kind::kShowRoles: {
            /*
             * Above operations are special operation.
             * can not get the space id via session,
             * Permission checking needs to be done in their executor.
             */
            return PermissionManager::canReadSpace(session, targetSpace);
        }
        case Sentence::Kind::kShowUsers:
        case Sentence::Kind::kShowSnapshots: {
            /**
             * Only GOD role can be show.
             */
            return session->isGod();
        }
        case Sentence::Kind::kChangePassword : {
            return true;
        }
        case Sentence::Kind::kSequential:
            LOG(FATAL) << "Impossible sequential sentences permission checking";
    }
    return false;
}
}  // namespace graph
}  // namespace nebula

