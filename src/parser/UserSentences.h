/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_USERSENTENCES_H_
#define PARSER_USERSENTENCES_H_

#include "common/interface/gen-cpp2/meta_types.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class RoleTypeClause final {
public:
    using RoleType = meta::cpp2::RoleType;

    explicit RoleTypeClause(RoleType roleType) {
        roleType_ = roleType;
    }


    RoleType getRoleType() const {
        return roleType_;
    }

    std::string toString() const;

private:
    RoleType      roleType_;
};


class AclItemClause final {
public:
    AclItemClause(bool isSet, std::string *spaceName) {
        isSet_ = isSet;

        spaceName_.reset(spaceName);
    }

    bool isSet() const {
        return isSet_;
    }

    void setRoleTypeClause(RoleTypeClause *type) {
        type_.reset(type);
    }

    RoleTypeClause::RoleType getRoleType() const {
        return type_->getRoleType();
    }

    const std::string* getSpaceName() const {
        return spaceName_.get();
    }

    std::string toString() const;

private:
    bool                                  isSet_;
    std::unique_ptr<RoleTypeClause>       type_;
    std::unique_ptr<std::string>          spaceName_;
};


class CreateUserSentence final : public Sentence {
public:
    CreateUserSentence(std::string *account, std::string *password, bool ifNotExists) {
        account_.reset(account);
        password_.reset(password);
        ifNotExists_ = ifNotExists;
        kind_ = Kind::kCreateUser;
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    const std::string* getPassword() const {
        return password_.get();
    }

    bool ifNotExists() const {
        return ifNotExists_;
    }

    std::string toString() const override;

private:
    bool                                  ifNotExists_{false};
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<std::string>          password_;
};


class AlterUserSentence final : public Sentence {
public:
    explicit AlterUserSentence(std::string *account, std::string *password) {
        account_.reset(account);
        password_.reset(password);
        kind_ = Kind::kAlterUser;
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    const std::string* getPassword() const {
        return password_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<std::string>          password_;
};


class DropUserSentence final : public Sentence {
public:
    explicit DropUserSentence(std::string *account, bool ifExists) {
        account_.reset(account);
        ifExists_ = ifExists;
        kind_ = Kind::kDropUser;
    }

    bool ifExists() const {
        return ifExists_;
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    std::string toString() const override;

private:
    bool                                  ifExists_{false};
    std::unique_ptr<std::string>          account_;
};


class ChangePasswordSentence final : public Sentence {
public:
    ChangePasswordSentence(std::string *account, std::string* oldPwd, std::string* newPwd) {
        account_.reset(account);
        newPwd_.reset(newPwd);
        oldPwd_.reset(oldPwd);
        kind_ = Kind::kChangePassword;
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    const std::string* getNewPwd() const {
        return newPwd_.get();
    }

    const std::string* getOldPwd() const {
        return oldPwd_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<std::string>          newPwd_;
    std::unique_ptr<std::string>          oldPwd_;
};


class GrantSentence final : public Sentence {
public:
    explicit GrantSentence(std::string *account) {
        account_.reset(account);
        kind_ = Kind::kGrant;
    }

    void setAclItemClause(AclItemClause* aclItemClause) {
        aclItemClause_.reset(aclItemClause);
    }

    const AclItemClause* getAclItemClause() const {
        return aclItemClause_.get();
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<AclItemClause>        aclItemClause_;
};


class RevokeSentence final : public Sentence {
public:
    explicit RevokeSentence(std::string *account) {
        account_.reset(account);
        kind_ = Kind::kRevoke;
    }

    void setAclItemClause(AclItemClause* aclItemClause) {
        aclItemClause_.reset(aclItemClause);
    }

    const AclItemClause* getAclItemClause() const {
        return aclItemClause_.get();
    }

    AclItemClause* mutableAclItemClause() {
        return aclItemClause_.get();
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<AclItemClause>        aclItemClause_;
};
}   // namespace nebula
#endif  // PARSER_USERSENTENCES_H_
