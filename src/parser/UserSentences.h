/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_USERSENTENCES_H_
#define PARSER_USERSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class WithUserOptItem final {
public:
    using OptVal = boost::variant<bool, int64_t>;
    enum OptionType : uint8_t {
        LOCK,
        MAX_QUERIES_PER_HOUR,
        MAX_UPDATES_PER_HOUR,
        MAX_CONNECTIONS_PER_HOUR,
        MAX_USER_CONNECTIONS,
    };

    WithUserOptItem(OptionType op, int64_t val) {
        optType_ = op;
        optVal_ = val;
    }

    WithUserOptItem(OptionType op, bool val) {
        optType_ = op;
        optVal_ = val;
    }

    OptVal getOptVal() const {
        switch (optType_) {
            case LOCK:
                return boost::get<bool>(optVal_);
            case MAX_QUERIES_PER_HOUR:
            case MAX_UPDATES_PER_HOUR:
            case MAX_CONNECTIONS_PER_HOUR:
            case MAX_USER_CONNECTIONS:
                return boost::get<int64_t>(optVal_);
        }
        return false;
    }

    static int64_t asInt(const OptVal &value) {
        return boost::get<int64_t>(value);
    }

    static bool asBool(const OptVal &value) {
        return boost::get<bool>(value);
    }

    OptionType getOptType() {
        return optType_;
    }

    std::string toString() const;

private:
    OptVal                           optVal_;
    OptionType                       optType_;
};

class WithUserOptList final {
public:
    void addOpt(WithUserOptItem *item) {
        items_.emplace_back(item);
    }

    std::vector<std::unique_ptr<WithUserOptItem>> getOpts() {
        return std::move(items_);
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<WithUserOptItem>>    items_;
};


class RoleTypeClause final {
public:
    enum RoleType : uint8_t {
        GOD,
        ADMIN,
        USER,
        GUEST
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

    const std::string* getSpaceName() {
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
    CreateUserSentence(std::string *account, std::string *password) {
        account_.reset(account);
        password_.reset(password);
        kind_ = Kind::kCreateUser;
    }

    const std::string* getAccount() {
        return account_.get();
    }

    const std::string* getPassword() {
        return password_.get();
    }

    void setMissingOk(bool missingOk) {
        missingOk_ = missingOk;
    }

    bool getMissingOk() {
        return missingOk_;
    }

    void setOpts(WithUserOptList* withUserOpts) {
        withUserOpts_.reset(withUserOpts);
    }

    std::vector<std::unique_ptr<WithUserOptItem>> getOpts() {
        if (withUserOpts_) {
            return withUserOpts_->getOpts();
        } else {
            return std::vector<std::unique_ptr<WithUserOptItem>>();
        }
    }

    std::string toString() const override;

private:
    bool                                  missingOk_{false};
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<std::string>          password_;
    std::unique_ptr<WithUserOptList>      withUserOpts_;
};


class AlterUserSentence final : public Sentence {
public:
    explicit AlterUserSentence(std::string *account) {
        account_.reset(account);
        kind_ = Kind::kAlterUser;
    }

    const std::string* getAccount() {
        return account_.get();
    }

    void setOpts(WithUserOptList* withUserOpts) {
        withUserOpts_.reset(withUserOpts);
    }

    std::vector<std::unique_ptr<WithUserOptItem>> getOpts() {
        return withUserOpts_->getOpts();
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

    void setMissingOk(bool missingOk) {
        missingOk_ = missingOk;
    }

    bool getMissingOk() {
        return missingOk_;
    }

    const std::string* getAccount() {
        return account_.get();
    }

    std::string toString() const override;

private:
    bool                                  missingOk_{false};
    std::unique_ptr<std::string>          account_;
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
        kind_ = Kind::kChangePassword;
    }

    const std::string* getAccount() {
        return account_.get();
    }

    const std::string* getNewPwd() {
        return newPwd_.get();
    }

    const std::string* getOldPwd() {
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

    const std::string* getAccount() {
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

    const std::string* getAccount() {
        return account_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<AclItemClause>        aclItemClause_;
};
}   // namespace nebula
#endif  // PARSER_USERSENTENCES_H_

