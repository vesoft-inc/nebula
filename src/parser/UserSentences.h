/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef PARSER_USERSENTENCES_H_
#define PARSER_GRAPH_USERSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class MissingOkClause final {
public:
    enum MissingType : uint8_t {
        IF_EXIST, IF_NOT_EXIST
    };

    explicit MissingOkClause(bool isMissing) {
        isMissing_ = isMissing;
    }

    bool isMissing() {
        return isMissing_;
    }

    void setType(MissingType type) {
        type_ = type;
    }

    MissingType getType() {
        return type_;
    }

    std::string toString() const;

private:
    bool isMissing_;
    MissingType type_;
};


class WithUserOptItem final {
public:
    enum OptionType : uint8_t {
        FIRST, LAST, EMAIL, PHONE
    };

    WithUserOptItem(OptionType op, std::string *val) {
        optType_ = op;
        optValue_.reset(val);
    }

    std::string* getValue() {
        return optValue_.get();
    }

    OptionType getOptType() {
        return optType_;
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>     optValue_;
    OptionType                       optType_;
};


class WithUserOptList final {
public:
    void addOpt(WithUserOptItem *item) {
        items_.emplace_back(item);
    }

    std::vector<std::unique_ptr<WithUserOptItem>> getOpt() {
        return std::move(items_);
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<WithUserOptItem>>    items_;
};


class RoleTypeClause final {
public:
    enum RoleType : uint8_t {
        GOD, ADMIN, USER, GUEST
    };

    explicit RoleTypeClause(RoleType roleType) {
        roleType_ = roleType;
    }

    RoleType getOptType() {
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

    bool isSet() {
        return isSet_;
    }

    void setRoleTypeClause(RoleTypeClause *type) {
        type_.reset(type);
    }

    RoleTypeClause::RoleType getRoleType() {
        return type_->getOptType();
    }

    std::string* getSpaceName() {
        return spaceName_.get();
    }

    std::string toString() const;

private:
    bool isSet_;
    std::unique_ptr<RoleTypeClause> type_;
    std::unique_ptr<std::string> spaceName_;
};


class CreateUserSentence final : public Sentence {
public:
    CreateUserSentence(std::string *account, std::string *password) {
        account_.reset(account);
        password_.reset(password);
        kind_ = Kind::kCreateUser;
    }

    bool isMissing() {
        return missingOkClause_->isMissing();
    }

    std::string* getAccount() {
        return account_.get();
    }

    std::string* getPasswoed() {
        return password_.get();
    }

    void setMissingOkClause(MissingOkClause* missingOkClause) {
        missingOkClause_.reset(missingOkClause);
    }

    void setMissingType(MissingOkClause::MissingType type) {
        missingOkClause_->setType(type);
    }

    void setOpts(WithUserOptList* withUserOpts) {
        withUserOpts_.reset(withUserOpts);
    }

    std::vector<std::unique_ptr<WithUserOptItem>> getOpts() {
        return withUserOpts_->getOpt();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<std::string>          password_;
    std::unique_ptr<WithUserOptList>      withUserOpts_;
    std::unique_ptr<MissingOkClause>      missingOkClause_;
};


class AlterUserSentence final : public Sentence {
public:
    explicit AlterUserSentence(std::string *account) {
        account_.reset(account);
        kind_ = Kind::kAlterUser;
    }

    std::string* getAccount() {
        return account_.get();
    }

    void setOpts(WithUserOptList* withUserOpts) {
        withUserOpts_.reset(withUserOpts);
    }

    std::vector<std::unique_ptr<WithUserOptItem>> getOpts() {
        return withUserOpts_->getOpt();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<WithUserOptList>      withUserOpts_;
};


class DropUserSentence final : public Sentence {
public:
    explicit DropUserSentence(std::string *account) {
        account_.reset(account);
        kind_ = Kind::kDropUser;
    }

    bool isMissing() {
        return missingOkClause_->isMissing();
    }

    void setMissingOkClause(MissingOkClause *missingOkClause) {
        missingOkClause_.reset(missingOkClause);
    }

    void setMissingType(MissingOkClause::MissingType type) {
        missingOkClause_->setType(type);
    }

    std::string* getAccount() {
        return account_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<MissingOkClause>      missingOkClause_;
};


class ChangePasswordSentence final : public Sentence {
public:
    ChangePasswordSentence(std::string *account, std::string* newPwd) {
        needVerify_ = false;
        account_.reset(account);
        newPwd_.reset(newPwd);
        kind_ = Kind::kChangePassword;
    }

    ChangePasswordSentence(std::string *account, std::string* oldPwd, std::string* newPwd) {
        needVerify_ = true;
        account_.reset(account);
        newPwd_.reset(newPwd);
        oldPwd_.reset(oldPwd);
    }

    std::string* getAccount() {
        return account_.get();
    }

    std::string* getNewPwd() {
        return newPwd_.get();
    }

    std::string* getOldPwd() {
        return oldPwd_.get();
    }

    bool needVerify() {
        return needVerify_;
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<std::string>          newPwd_;
    std::unique_ptr<std::string>          oldPwd_;
    bool                                  needVerify_;
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

    AclItemClause* getAclItemClause() {
        return aclItemClause_.get();
    }

    std::string* getAccount() {
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

    AclItemClause* getAclItemClause() {
        return aclItemClause_.get();
    }

    std::string* getAccount() {
        return account_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<AclItemClause>        aclItemClause_;
};
}   // namespace nebula
#endif  // PARSER_USERSENTENCES_H_

