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
    std::string errMsg;
    errMsg.reserve(128);
#define ERROR_TEMPLATE(err) \
    case k##err: { errMsg += (#err  ": ");} \
    break;

    switch (code()) {
        // OK
        ERROR_TEMPLATE(Ok)
        ERROR_TEMPLATE(Inserted)
        // 1xx, for genral errors
        ERROR_TEMPLATE(Error)
        ERROR_TEMPLATE(NoSuchFile)
        ERROR_TEMPLATE(NotSupported)
        // 2xx, for grah engine errors
        ERROR_TEMPLATE(SyntaxError)
        ERROR_TEMPLATE(StatementEmpty)
        // 3xx, for stoage engine errors
        ERROR_TEMPLATE(KeyNotFound)
        // 4xx, for met service errors
        ERROR_TEMPLATE(SpaceNotFound)
        ERROR_TEMPLATE(HostNotFound)
        ERROR_TEMPLATE(TagNotFound)
        ERROR_TEMPLATE(EdgeNotFound)
        ERROR_TEMPLATE(UserNotFound)
        ERROR_TEMPLATE(LeaderChanged)
        ERROR_TEMPLATE(Balanced)
        ERROR_TEMPLATE(IndexNotFound)
        ERROR_TEMPLATE(PartNotFound)
        // 5xx for useror permission error
        ERROR_TEMPLATE(PermissionError)
#undef ERROR_TEMPLATE
        default: {
            char tmp[64];
            snprintf(tmp, sizeof(tmp), "Unknown error(%hu): ", static_cast<uint16_t>(code()));
            errMsg += tmp;
            break;
        }
    }
    if (ok()) {
        // nullptr as OK too
        return errMsg;
    }
    errMsg.append(&state_[kHeaderSize], size());
    return errMsg;
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
