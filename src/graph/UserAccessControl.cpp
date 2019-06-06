/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "UserAccessControl.h"

namespace nebula {
namespace graph {

#define    ACL_GO                 (1<<0)
#define    ACL_SET                (1<<1)
#define    ACL_PIPE               (1<<2)
#define    ACL_USE                (1<<3)
#define    ACL_MATCH              (1<<4)
#define    ACL_ASSIGNMENT         (1<<5)
#define    ACL_CREATETAG          (1<<6)
#define    ACL_ALTERTAG           (1<<7)
#define    ACL_CREATEEDGE         (1<<8)
#define    ACL_ALTEREDGE          (1<<9)
#define    ACL_DESCRIBETAG        (1<<10)
#define    ACL_DESCRIBEEDGE       (1<<11)
#define    ACL_DROPTAG            (1<<12)
#define    ACL_DROPEDGE           (1<<13)
#define    ACL_INSERTVERTEX       (1<<14)
#define    ACL_INSERTEDGE         (1<<15)
#define    ACL_SHOW               (1<<16)
#define    ACL_DELETEVERTEX       (1<<17)
#define    ACL_DELETEEDGE         (1<<18)
#define    ACL_FIND               (1<<19)
#define    ACL_ADDHOSTS           (1<<20)
#define    ACL_REMOVEHOSTS        (1<<21)
#define    ACL_CREATESPACE        (1<<22)
#define    ACL_DROPSPACE          (1<<23)
#define    ACL_DESCRIBESPACE      (1<<24)
#define    ACL_YIELD              (1<<25)
#define    ACL_CREATEUSER         (1<<26)
#define    ACL_DROPUSER           (1<<27)
#define    ACL_ALTERUSER          (1<<28)
#define    ACL_GRANT              (1<<29)
#define    ACL_REVOKE             (1<<30)
#define    ACL_CHANGEPASSWORD     (1<<31)

using nebula::meta::cpp2::RoleType;
using nebula::meta::cpp2::ErrorCode;

int32_t UserAccessControl::getACLType(Sentence::Kind kind) {
    switch (kind) {
        case Sentence::Kind::kGo:
            return ACL_GO;
        case Sentence::Kind::kSet:
            return ACL_SET;
        case Sentence::Kind::kPipe:
            return ACL_PIPE;
        case Sentence::Kind::kUse:
            return ACL_USE;
        case Sentence::Kind::kMatch:
            return ACL_MATCH;
        case Sentence::Kind::kAssignment:
            return ACL_ASSIGNMENT;
        case Sentence::Kind::kCreateTag:
            return ACL_CREATETAG;
        case Sentence::Kind::kAlterTag:
            return ACL_ALTERTAG;
        case Sentence::Kind::kCreateEdge:
            return ACL_CREATEEDGE;
        case Sentence::Kind::kAlterEdge:
            return ACL_ALTEREDGE;
        case Sentence::Kind::kDescribeTag:
            return ACL_DESCRIBETAG;
        case Sentence::Kind::kDescribeEdge:
            return ACL_DESCRIBEEDGE;
        case Sentence::Kind::kDropTag:
            return ACL_DROPTAG;
        case Sentence::Kind::kDropEdge:
            return ACL_DROPEDGE;
        case Sentence::Kind::kInsertVertex:
            return ACL_INSERTVERTEX;
        case Sentence::Kind::kInsertEdge:
            return ACL_INSERTEDGE;
        case Sentence::Kind::kShow:
            return ACL_SHOW;
        case Sentence::Kind::kDeleteVertex:
            return ACL_DELETEVERTEX;
        case Sentence::Kind::kDeleteEdge:
            return ACL_DELETEEDGE;
        case Sentence::Kind::kFind:
            return ACL_FIND;
        case Sentence::Kind::kAddHosts:
            return ACL_ADDHOSTS;
        case Sentence::Kind::kRemoveHosts:
            return ACL_REMOVEHOSTS;
        case Sentence::Kind::kCreateSpace:
            return ACL_CREATESPACE;
        case Sentence::Kind::kDropSpace:
            return ACL_DROPSPACE;
        case Sentence::Kind::kDescribeSpace:
            return ACL_DESCRIBESPACE;
        case Sentence::Kind::kYield:
            return ACL_YIELD;
        case Sentence::Kind::kCreateUser:
            return ACL_CREATEUSER;
        case Sentence::Kind::kDropUser:
            return ACL_DROPUSER;
        case Sentence::Kind::kAlterUser:
            return ACL_ALTERUSER;
        case Sentence::Kind::kGrant:
            return ACL_GRANT;
        case Sentence::Kind::kRevoke:
            return ACL_REVOKE;
        case Sentence::Kind::kChangePassword:
            return ACL_CHANGEPASSWORD;
        default:
            return 0;
    }
}


int32_t UserAccessControl::getACLMode(RoleType type) {
    //  Operation and permission define reference UserAccessControl.h
    int32_t acl = 0;
    switch (type) {
        case RoleType::GOD:
            //  God has all privileges
            acl |= (ACL_ADDHOSTS
                   |ACL_REMOVEHOSTS
                   |ACL_CREATESPACE
                   |ACL_CREATEUSER
                   |ACL_DROPUSER);
        case RoleType::ADMIN:
            acl |= (ACL_CREATETAG
                   |ACL_ALTERTAG
                   |ACL_CREATEEDGE
                   |ACL_ALTEREDGE
                   |ACL_DROPTAG
                   |ACL_DROPEDGE
                   |ACL_DROPSPACE
                   |ACL_DESCRIBESPACE
                   |ACL_GRANT
                   |ACL_REVOKE);
        case RoleType::GUEST:
            acl |= (ACL_INSERTVERTEX
                   |ACL_INSERTEDGE
                   |ACL_DELETEVERTEX
                   |ACL_DELETEEDGE);
        case RoleType::USER:
            acl |= (ACL_GO
                   |ACL_SET
                   |ACL_PIPE
                   |ACL_USE
                   |ACL_MATCH
                   |ACL_ASSIGNMENT
                   |ACL_DESCRIBETAG
                   |ACL_DESCRIBEEDGE
                   |ACL_SHOW
                   |ACL_FIND
                   |ACL_YIELD
                   |ACL_ALTERUSER
                   |ACL_CHANGEPASSWORD);
            break;
        default:
            return 0;
    }
    return acl;
}

int32_t UserAccessControl::checkACL(RoleType type, Sentence::Kind op) {
    return getACLMode(type) & getACLType(op);
}

Status UserAccessControl::checkPerms(GraphSpaceID spaceID,
                                        UserID userId,
                                        Sentence::Kind op,
                                        meta::MetaClient* mc) {
    auto userRet = mc->getUserNameByIdFromCache(userId);
    if (!userRet.ok()) {
        return Status::Error("User not found : %d", userId);
    }

    if (mc->checkIsGodUserInCache(userRet.value())) {
        return Status::OK();
    }

    auto role = mc->getRoleFromCache(spaceID, userId);
    if (checkACL(role, op) == 0) {
        return Status::Error("Access denied for userId : %d", userId);
    }
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula

