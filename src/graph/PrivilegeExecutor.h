/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef GRAPH_PRIVILEGEEXECUTOR_H_
#define GRAPH_PRIVILEGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "meta/SchemaManager.h"
#include "common/permission/PermissionManager.h"

namespace nebula {
namespace graph {
class PrivilegeUtils final {
public:
    static nebula::cpp2::RoleType toRoleType(RoleTypeClause::RoleType roleType) {
        /*
         * set the init value to GUEST for compile error.
         */
        nebula::cpp2::RoleType type = nebula::cpp2::RoleType::GUEST;
        switch (roleType) {
            case RoleTypeClause::RoleType::GOD : {
                type = nebula::cpp2::RoleType::GOD;
                break;
            }
            case RoleTypeClause::RoleType::ADMIN : {
                type = nebula::cpp2::RoleType::ADMIN;
                break;
            }
            case RoleTypeClause::RoleType::DBA : {
                type = nebula::cpp2::RoleType::DBA;
                break;
            }
            case RoleTypeClause::RoleType::USER : {
                type = nebula::cpp2::RoleType::USER;
                break;
            }
            case RoleTypeClause::RoleType::GUEST : {
                type = nebula::cpp2::RoleType::GUEST;
                break;
            }
        }
        return type;
    }
};

class GrantExecutor final : public Executor {
public:
    GrantExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GrantExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;
private:
    GrantSentence                                   *sentence_{nullptr};
};

class RevokeExecutor final : public Executor {
public:
    RevokeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RevokeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;
private:
    RevokeSentence                                   *sentence_{nullptr};
};
}   // namespace graph
}   // namespace nebula
#endif   // GRAPH_PRIVILEGEEXECUTOR_H_
