/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "parser/UserSentences.h"

namespace nebula {

std::string MissingOkClause::toString() const {
    if (isMissing_) {
        switch (type_) {
            case MissingType::IF_EXIST :
                return std::string("IF EXISTS");
            case MissingType::IF_NOT_EXIST :
                return std::string("IF NOT EXISTS");
            default:
                FLOG_FATAL("Type illegal");
        }
    }
    return "";
}


std::string WithUserOptItem::toString() const {
    switch (optType_) {
        case OptionType::FIRST :
            return folly::stringPrintf("FIRSTNAME \"%s\"", optValue_.get()->data());
        case OptionType::LAST :
            return folly::stringPrintf("LASTNAME \"%s\"", optValue_.get()->data());
        case OptionType::EMAIL :
            return folly::stringPrintf("EMAIL \"%s\"", optValue_.get()->data());
        case OptionType::PHONE :
            return folly::stringPrintf("PHONE \"%s\"", optValue_.get()->data());
        default:
            FLOG_FATAL("User parameter illegal");
    }
    return "Unknown";
}


std::string WithUserOptList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (uint32_t i = 0; i < items_.size(); i++) {
        auto &item = items_[i];
        if (i > 0) {
            buf += ", ";
        }
        buf += item->toString();
    }
    return buf;
}


std::string RoleTypeClause::toString() const {
    switch (roleType_) {
        case RoleType::GOD :
            return std::string("GOD");
        case RoleType::ADMIN :
            return std::string("ADMIN");
        case RoleType::USER :
            return std::string("USER");
        case RoleType::GUEST :
            return std::string("GUEST");
        default:
            FLOG_FATAL("Role type illegal");
    }
    return "Unknown";
}


std::string AclItemClause::toString() const {
    std::string buf;
    buf.reserve(256);
    if (isSet_) {
        buf = "ROLE ";
    }
    buf += type_->toString();
    buf += " ON ";
    buf += spaceName_.get()->data();
    return buf;
}


std::string CreateUserSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "CREATE USER ";
    buf += missingOkClause_->toString().data();
    buf += " ";
    buf += account_->data();
    buf += " WITH PASSWORD \"";
    buf += password_->data();
    buf += "\" ";
    if (withUserOpts_) {
        buf += ", ";
        buf += withUserOpts_->toString();
    }
    return buf;
}


std::string AlterUserSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "ALTER USER ";
    buf += account_->data();
    buf += " WITH ";
    buf += withUserOpts_->toString();
    return buf;
}


std::string DropUserSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "DROP USER ";
    buf += missingOkClause_->toString();
    buf += " ";
    buf += account_->data();
    return buf;
}


std::string ChangePasswordSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "CHANGE PASSWORD ";
    buf += account_->data();
    if (needVerify_) {
        buf += " FROM \"";
        buf += oldPwd_->data();
        buf += "\" ";
    }
    buf += "TO \"";
    buf += newPwd_->data();
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

    buf += account_->data();
    return buf;
}


std::string RevokeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "REVOKE ";
    buf += aclItemClause_->toString();
    buf += " FROM ";
    buf += account_->data();
    return buf;
}
}  // namespace nebula
