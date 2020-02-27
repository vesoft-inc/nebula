/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/UserSentences.h"

namespace nebula {

std::string UserLoginType::toString() const {
    std::string buf;
    switch (loginType_) {
        case LoginType::PASSWORD : {
            buf += "PASSWORD ";
            buf += "\"" + *password_ + "\"";
            break;
        }
        case LoginType::LDAP : {
            buf += "LDAP";
            break;
        }
    }
    return buf;
}

std::string WithUserOptItem::toString() const {
    switch (optType_) {
        case OptionType::LOCK: {
            if (asBool()) {
                return std::string("ACCOUNT LOCK");
            }
            return std::string("ACCOUNT UNLOCK");
        }
        case OptionType::MAX_QUERIES_PER_HOUR:
            return folly::stringPrintf("MAX_QUERIES_PER_HOUR %ld", asInt());
        case OptionType::MAX_UPDATES_PER_HOUR:
            return folly::stringPrintf("MAX_UPDATES_PER_HOUR %ld", asInt());
        case OptionType::MAX_CONNECTIONS_PER_HOUR:
            return folly::stringPrintf("MAX_CONNECTIONS_PER_HOUR %ld", asInt());
        case OptionType::MAX_USER_CONNECTIONS:
            return folly::stringPrintf("MAX_USER_CONNECTIONS %ld", asInt());
        default:
            return "Unknown";
    }
}


std::string WithUserOptList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto& item : items_) {
        if (!buf.empty()) {
            buf += ", ";
        }
        buf += item->toString();
    }
    return buf;
}


std::string RoleTypeClause::toString() const {
    switch (roleType_) {
        case RoleType::GOD:
            return std::string("GOD");
        case RoleType::ADMIN:
            return std::string("ADMIN");
        case RoleType::DBA:
            return std::string("DBA");
        case RoleType::USER:
            return std::string("USER");
        case RoleType::GUEST:
            return std::string("GUEST");
        default:
            return "Unknown";
    }
}


std::string AclItemClause::toString() const {
    std::string buf;
    buf.reserve(256);
    if (isSet_) {
        buf = "ROLE ";
    }
    buf += type_->toString();
    buf += " ON ";
    buf += *spaceName_;
    return buf;
}


std::string CreateUserSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "CREATE USER ";
    if (ifNotExists_) {
        buf += "IF NOT EXISTS ";
    }
    buf += *account_;
    buf += " WITH ";
    if (userLoginType_) {
        buf += userLoginType_->toString();
    }
    if (withUserOpts_) {
        buf += " , ";
        buf += withUserOpts_->toString();
    }
    return buf;
}


std::string AlterUserSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "ALTER USER ";
    buf += *account_;
    buf += " WITH ";
    if (userLoginType_) {
        buf += userLoginType_->toString();
    }
    if (userLoginType_ && withUserOpts_) {
        buf += ", ";
    }
    if (withUserOpts_) {
        buf += withUserOpts_->toString();
    }
    return buf;
}


std::string DropUserSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "DROP USER ";
    if (ifExists_) {
        buf += "IF EXISTS ";
    }
    buf += *account_;
    return buf;
}


std::string ChangePasswordSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "CHANGE PASSWORD ";
    buf += *account_;
    buf += " FROM \"";
    buf += *oldPwd_;
    buf += "\" TO \"";
    buf += *newPwd_;
    buf += "\" ";
    buf.resize(buf.size()-1);
    return buf;
}


std::string GrantSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "GRANT ";
    buf += aclItemClause_->toString();
    buf += " TO ";

    buf += *account_;
    return buf;
}


std::string RevokeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "REVOKE ";
    buf += aclItemClause_->toString();
    buf += " FROM ";
    buf += *account_;
    return buf;
}
}  // namespace nebula
