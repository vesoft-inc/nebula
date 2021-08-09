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
Status PermissionCheck::permissionCheck(ClientSession *session,
                                        Sentence* sentence,
                                        ValidateContext *vctx,
                                        GraphSpaceID targetSpace) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kUnknown: {
            return Status::Error("Unknown sentence");
        }
        case Sentence::Kind::kUse:
        case Sentence::Kind::kDescribeSpace: {
            /**
             * Use space and Describe space are special operations.
             * Permission checking needs to be done in their executor.
             * skip the check at here.
             */
            return Status::OK();
        }
        case Sentence::Kind::kCreateSpace:
        case Sentence::Kind::kDropSpace:
        case Sentence::Kind::kCreateSnapshot:
        case Sentence::Kind::kDropSnapshot:
        case Sentence::Kind::kAddGroup:
        case Sentence::Kind::kDropGroup:
        case Sentence::Kind::kDescribeGroup:
        case Sentence::Kind::kListGroups:
        case Sentence::Kind::kAddZoneIntoGroup:
        case Sentence::Kind::kDropZoneFromGroup:
        case Sentence::Kind::kAddZone:
        case Sentence::Kind::kDropZone:
        case Sentence::Kind::kDescribeZone:
        case Sentence::Kind::kListZones:
        case Sentence::Kind::kAddHostIntoZone:
        case Sentence::Kind::kDropHostFromZone:
        case Sentence::Kind::kBalance:
        case Sentence::Kind::kAdminJob:
        case Sentence::Kind::kShowConfigs:
        case Sentence::Kind::kSetConfig:
        case Sentence::Kind::kGetConfig:
        case Sentence::Kind::kIngest:
        case Sentence::Kind::kDownload:
        case Sentence::Kind::kSignOutTSService:
        case Sentence::Kind::kSignInTSService: {
            return PermissionManager::canWriteSpace(session);
        }
        case Sentence::Kind::kCreateTag:
        case Sentence::Kind::kAlterTag:
        case Sentence::Kind::kCreateEdge:
        case Sentence::Kind::kAlterEdge:
        case Sentence::Kind::kDropTag:
        case Sentence::Kind::kDropEdge:
        case Sentence::Kind::kCreateTagIndex:
        case Sentence::Kind::kCreateEdgeIndex:
        case Sentence::Kind::kCreateFTIndex:
        case Sentence::Kind::kDropTagIndex:
        case Sentence::Kind::kDropEdgeIndex:
        case Sentence::Kind::kDropFTIndex:
        case Sentence::Kind::kAddListener:
        case Sentence::Kind::kRemoveListener: {
            return PermissionManager::canWriteSchema(session, vctx);
        }
        case Sentence::Kind::kCreateUser:
        case Sentence::Kind::kDropUser:
        case Sentence::Kind::kAlterUser: {
            return PermissionManager::canWriteUser(session);
        }
        case Sentence::Kind::kRevoke:
        case Sentence::Kind::kGrant: {
            /**
             * Use grant and revoke are special operations.
             * Because have not found the target space id and target role
             * so permission checking needs to be done in their executor.
             * skip the check at here.
             */
            return Status::OK();
        }
        case Sentence::Kind::kInsertVertices :
        case Sentence::Kind::kUpdateVertex :
        case Sentence::Kind::kInsertEdges :
        case Sentence::Kind::kUpdateEdge :
        case Sentence::Kind::kDeleteVertices :
        case Sentence::Kind::kDeleteEdges : {
            return PermissionManager::canWriteData(session, vctx);
        }
        case Sentence::Kind::kDescribeTag:
        case Sentence::Kind::kDescribeEdge:
        case Sentence::Kind::kDescribeTagIndex:
        case Sentence::Kind::kDescribeEdgeIndex:
        case Sentence::Kind::kGo:
        case Sentence::Kind::kSet:
        case Sentence::Kind::kPipe:
        case Sentence::Kind::kMatch:
        case Sentence::Kind::kAssignment:
        case Sentence::Kind::kLookup:
        case Sentence::Kind::kYield:
        case Sentence::Kind::kOrderBy:
        case Sentence::Kind::kFetchVertices:
        case Sentence::Kind::kFetchEdges:
        case Sentence::Kind::kFindPath:
        case Sentence::Kind::kGetSubgraph:
        case Sentence::Kind::kLimit:
        case Sentence::Kind::kGroupBy:
        case Sentence::Kind::kReturn: {
            return PermissionManager::canReadSchemaOrData(session, vctx);
        }
        case Sentence::Kind::kShowParts:
        case Sentence::Kind::kShowTags:
        case Sentence::Kind::kShowEdges:
        case Sentence::Kind::kShowStats:
        case Sentence::Kind::kShowTagIndexes:
        case Sentence::Kind::kShowEdgeIndexes:
        case Sentence::Kind::kShowTagIndexStatus:
        case Sentence::Kind::kShowEdgeIndexStatus:
        case Sentence::Kind::kShowCreateTag:
        case Sentence::Kind::kShowCreateEdge:
        case Sentence::Kind::kShowCreateTagIndex:
        case Sentence::Kind::kShowCreateEdgeIndex:
        case Sentence::Kind::kShowListener:
        case Sentence::Kind::kShowFTIndexes: {
            /**
             * Above operations can get the space id via session,
             * so the permission same with canReadSchemaOrData.
             * They've been checked by "USE SPACE", so here skip the check.
             */
            return Status::OK();
        }
        case Sentence::Kind::kShowCharset:
        case Sentence::Kind::kShowCollation:
        case Sentence::Kind::kShowGroups:
        case Sentence::Kind::kShowZones:
        case Sentence::Kind::kShowHosts: {
            /**
             * all roles can be show for above operations.
             */
            return Status::OK();
        }
        case Sentence::Kind::kShowSpaces: {
            /*
             * Above operations are special operation.
             * can not get the space id via session,
             * Permission checking needs to be done in their executor.
             */
            return Status::OK();
        }
        case Sentence::Kind::kShowCreateSpace:
        case Sentence::Kind::kShowRoles: {
            return PermissionManager::canReadSpace(session, targetSpace);
        }
        case Sentence::Kind::kShowUsers:
        case Sentence::Kind::kShowSnapshots:
        case Sentence::Kind::kShowTSClients:
        case Sentence::Kind::kShowSessions: {
            /**
             * Only GOD role can be show.
             */
            if (session->isGod()) {
                return Status::OK();
            } else {
                return Status::PermissionError("No permission to show users/snapshots/textClients");
            }
        }
        case Sentence::Kind::kChangePassword: {
            return Status::OK();
        }
        case Sentence::Kind::kExplain:
            // everyone could explain
            return Status::OK();
        case Sentence::Kind::kSequential: {
            // No permission checking for sequential sentence.
            return Status::OK();
        }
        case Sentence::Kind::kShowQueries:
        case Sentence::Kind::kKillQuery: {
            return Status::OK();
        }
    }
    LOG(ERROR) << "Impossible permission checking for sentence " << sentence->kind();
    return Status::Error("Impossible permission checking for sentence %d.",
                         static_cast<int>(sentence->kind()));
}
}  // namespace graph
}  // namespace nebula
