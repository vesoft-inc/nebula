/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Status.h"

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


std::string Status::toString() const {
    if (code() == kOk) {
        return "OK";
    }
    char tmp[64];
    const char *str;
    switch (code()) {
        case kError:
            str = "";
            break;
        case kSyntaxError:
            str = "SyntaxError: ";
            break;
        case kPermissionError:
            str = "PermissionError: ";
            break;
        case kSpaceNotFound:
            str = "Space not found";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unknown error(%hu): ", static_cast<uint16_t>(code()));
            str = tmp;
            break;
    }
    std::string result(str);
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

}   // namespace nebula
