/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/base/Status.h"

namespace nebula {

Status::Status(Code code, folly::StringPiece msg) {
    const uint16_t size = msg.size();
    auto state = std::unique_ptr<char[]>(new char[size + kHeaderSize]);
    auto *header = reinterpret_cast<Header*>(state.get());
    header->size_ = size;
    header->code_ = code;
    ::memcpy(&state[kHeaderSize], msg.data(), size);
    state_ = std::move(state);
}

std::string Status::message() const {
    if (state_ == nullptr) return "";
    return std::string(&state_[kHeaderSize], size());
}

std::string Status::toString() const {
    Code code = this->code();
    if (code == kOk) {
        return "OK";
    }
    std::string result(toString(code));
    result.append(&state_[kHeaderSize], size());
    return result;
}

std::unique_ptr<const char[]> Status::copyState(const char *state) {
    const auto size = *reinterpret_cast<const uint16_t*>(state);
    const auto total = size + kHeaderSize;
    auto result = std::unique_ptr<char[]>(new char[total]);
    ::memcpy(&result[0], state, total);
    return result;
}


std::string Status::format(const char *fmt, va_list args) {
    char result[256];
    vsnprintf(result, sizeof(result), fmt, args);
    return result;
}

// static
const char *Status::toString(Code code) {
    switch (code) {
        case kOk:
            return "OK";
        case kInserted:
            return "Inserted: ";
        case kError:
            return "";
        case kNoSuchFile:
            return "NoSuchFile: ";
        case kNotSupported:
            return "NotSupported: ";
        case kSyntaxError:
            return "SyntaxError: ";
        case kStatementEmpty:
            return "StatementEmpty: ";
        case kSemanticError:
            return "SemanticError: ";
        case kKeyNotFound:
            return "KeyNotFound: ";
        case kPartialSuccess:
            return "PartialSuccess: ";
        case kSpaceNotFound:
            return "SpaceNotFound: ";
        case kHostNotFound:
            return "HostNotFound: ";
        case kTagNotFound:
            return "TagNotFound: ";
        case kEdgeNotFound:
            return "EdgeNotFound: ";
        case kGroupNotFound:
            return "GroupNotFound: ";
        case kZoneNotFound:
            return "ZoneNotFound: ";
        case kUserNotFound:
            return "UserNotFound: ";
        case kLeaderChanged:
            return "LeaderChanged: ";
        case kBalanced:
            return "Balanced: ";
        case kIndexNotFound:
            return "IndexNotFound: ";
        case kPartNotFound:
            return "PartNotFound: ";
        case kPermissionError:
            return "PermissionError: ";
        case kListenerNotFound:
            return "ListenerNotFound";
    }
    DLOG(FATAL) << "Invalid status code: " << static_cast<uint16_t>(code);
    return "";
}

}   // namespace nebula
