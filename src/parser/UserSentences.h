/* Copyright (c) 2018 vesoft inc. All rights reserved.
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

class UserLoginType final {
public:
    enum LoginType : uint8_t {
        PASSWORD,
        LDAP
    };

    explicit UserLoginType(std::string *password) {
        loginType_ = LoginType::PASSWORD;
        password_.reset(password);
    }

    UserLoginType() {
        loginType_ = LoginType::LDAP;
    }

    LoginType getLoginType() const {
        return loginType_;
    }

    std::string* getPassword() const {
        return password_.get();
    }

    std::string toString() const;

private:
    LoginType                     loginType_;
    std::unique_ptr<std::string>  password_;
};

class WithUserOptItem final {
public:
    using OptVal = boost::variant<bool, int64_t>;
    enum OptionType : uint8_t {
        LOCK,
        MAX_QUERIES_PER_HOUR,
        MAX_UPDATES_PER_HOUR,
        MAX_CONNECTIONS_PER_HOUR,
        MAX_USER_CONNECTIONS
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

    int64_t asInt() const {
        return boost::get<int64_t>(optVal_);
    }

   bool asBool() const {
        return boost::get<bool>(optVal_);
    }

    OptionType getOptType() const {
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

    std::vector<WithUserOptItem*> getOpts() {
        std::vector<WithUserOptItem*> result;
        result.resize(items_.size());
        auto get = [](const auto&item) { return item.get(); };
        std::transform(items_.begin(), items_.end(), result.begin(), get);
        return result;
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
        DBA,
        USER,
        GUEST
    };

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
    CreateUserSentence(std::string *account, bool ifNotExists) {
        account_.reset(account);
        ifNotExists_ = ifNotExists;
        kind_ = Kind::kCreateUser;
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    bool ifNotExists() const {
        return ifNotExists_;
    }

    void setOpts(WithUserOptList* withUserOpts) {
        withUserOpts_.reset(withUserOpts);
    }

    std::vector<WithUserOptItem*> getOpts() const {
        if (withUserOpts_) {
            return withUserOpts_->getOpts();
        } else {
            return std::vector<WithUserOptItem*>(0);
        }
    }

    void setLoginType(UserLoginType* userLoginType) {
        userLoginType_.reset(userLoginType);
    }

    UserLoginType* getLoginType() const {
        return userLoginType_.get();
    }

    std::string toString() const override;

private:
    bool                                  ifNotExists_{false};
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<UserLoginType>        userLoginType_;
    std::unique_ptr<WithUserOptList>      withUserOpts_;
};


class AlterUserSentence final : public Sentence {
public:
    explicit AlterUserSentence(std::string *account) {
        account_.reset(account);
        kind_ = Kind::kAlterUser;
    }

    const std::string* getAccount() const {
        return account_.get();
    }

    void setOpts(WithUserOptList* withUserOpts) {
        withUserOpts_.reset(withUserOpts);
    }

    std::vector<WithUserOptItem*> getOpts() const {
        if (withUserOpts_) {
            return withUserOpts_->getOpts();
        } else {
            return std::vector<WithUserOptItem*>(0);
        }
    }

    void setLoginType(UserLoginType* userLoginType) {
        userLoginType_.reset(userLoginType);
    }

    UserLoginType* getLoginType() const {
        return userLoginType_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>          account_;
    std::unique_ptr<UserLoginType>        userLoginType_;
    std::unique_ptr<WithUserOptList>      withUserOpts_;
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

