/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_STATUS_H_
#define COMMON_BASE_STATUS_H_

#include "common/base/Base.h"

/**
 * Status is modeled on the one from levelDB, beyond that,
 * this one adds support on move semantics and formatted error messages.
 *
 * Status is as cheap as raw pointers in the successful case,
 * without any heap memory allocations.
 */

namespace nebula {

template <typename T>
class StatusOr;

class Status final {
public:
    Status() = default;

    ~Status() = default;

    Status(const Status &rhs) {
        state_ = rhs.state_ == nullptr ? nullptr : copyState(rhs.state_.get());
    }

    Status& operator=(const Status &rhs) {
        // `state_ == rhs.state_' means either `this == &rhs',
        // or both `*this' and `rhs' are OK
        if (state_ != rhs.state_) {
            state_ = rhs.state_ == nullptr ? nullptr : copyState(rhs.state_.get());
        }
        return *this;
    }

    Status(Status &&rhs) noexcept {
        state_ = std::move(rhs.state_);
    }

    Status& operator=(Status &&rhs) noexcept {
        // `state_ == rhs.state_' means either `this == &rhs',
        // or both `*this' and `rhs' are OK
        if (state_ != rhs.state_) {
            state_ = std::move(rhs.state_);
        }
        return *this;
    }

    static Status from(const Status &s) {
        return s;
    }

    template <typename T>
    static Status from(StatusOr<T> &&s) {
        return std::move(s).status();
    }

    template <typename T>
    static Status from(const StatusOr<T> &s) {
        return s.status();
    }

    bool operator==(const Status &rhs) const {
        // `state_ == rhs.state_' means either `this == &rhs',
        // or both `*this' and `rhs' are OK
        if (state_ == rhs.state_) {
            return true;
        }
        return code() == rhs.code();
    }

    bool operator!=(const Status &rhs) const {
        return !(*this == rhs);
    }

    bool ok() const {
        return state_ == nullptr;
    }

    static Status OK() {
        return Status();
    }

#define STATUS_GENERATOR(ERROR)                         \
    static Status ERROR() {                             \
        return Status(k##ERROR, "");                    \
    }                                                   \
                                                        \
    static Status ERROR(folly::StringPiece msg) {       \
        return Status(k##ERROR, msg);                   \
    }                                                   \
                                                        \
    static Status ERROR(const char *fmt, ...)           \
        __attribute__((format(printf, 1, 2))) {         \
        va_list args;                                   \
        va_start(args, fmt);                            \
        auto msg = format(fmt, args);                   \
        va_end(args);                                   \
        return Status(k##ERROR, msg);                   \
    }                                                   \
                                                        \
    bool is##ERROR() const {                            \
        return code() == k##ERROR;                      \
    }
    // Some succeeded codes
    STATUS_GENERATOR(Inserted);

    // General errors
    STATUS_GENERATOR(Error);
    STATUS_GENERATOR(NoSuchFile);
    STATUS_GENERATOR(NotSupported);

    // Graph engine errors
    STATUS_GENERATOR(SyntaxError);
    STATUS_GENERATOR(SemanticError);
    // Nothing is executed When command is comment
    STATUS_GENERATOR(StatementEmpty);

    // Storage engine errors
    STATUS_GENERATOR(KeyNotFound);
    STATUS_GENERATOR(PartialSuccess);

    // Meta engine errors
    // TODO(dangleptr) we could use ErrorOr to replace SpaceNotFound here.
    STATUS_GENERATOR(SpaceNotFound);
    STATUS_GENERATOR(HostNotFound);
    STATUS_GENERATOR(TagNotFound);
    STATUS_GENERATOR(EdgeNotFound);
    STATUS_GENERATOR(UserNotFound);
    STATUS_GENERATOR(IndexNotFound);
    STATUS_GENERATOR(GroupNotFound);
    STATUS_GENERATOR(ZoneNotFound);
    STATUS_GENERATOR(LeaderChanged);
    STATUS_GENERATOR(Balanced);
    STATUS_GENERATOR(PartNotFound);
    STATUS_GENERATOR(ListenerNotFound);

    // User or permission errors
    STATUS_GENERATOR(PermissionError);

#undef STATUS_GENERATOR

    std::string toString() const;

    friend std::ostream& operator<<(std::ostream &os, const Status &status);

    // If some kind of error really needs to be distinguished with others using a specific
    // code, other than a general code and specific msg, you could add a new code below,
    // e.g. kSomeError, and add the cooresponding STATUS_GENERATOR(SomeError)
    enum Code : uint16_t {
        // OK
        kOk                     = 0,
        kInserted               = 1,
        // 1xx, for general errors
        kError                  = 101,
        kNoSuchFile             = 102,
        kNotSupported           = 103,
        // 2xx, for graph engine errors
        kSyntaxError            = 201,
        kStatementEmpty         = 202,
        kSemanticError          = 203,
        // 3xx, for storage engine errors
        kKeyNotFound            = 301,
        kPartialSuccess         = 302,
        // 4xx, for meta service errors
        kSpaceNotFound          = 404,
        kHostNotFound           = 405,
        kTagNotFound            = 406,
        kEdgeNotFound           = 407,
        kUserNotFound           = 408,
        kLeaderChanged          = 409,
        kBalanced               = 410,
        kIndexNotFound          = 411,
        kPartNotFound           = 412,
        kGroupNotFound          = 413,
        kZoneNotFound           = 414,
        kListenerNotFound       = 415,
        // 5xx for user or permission error
        kPermissionError        = 501,
    };

    Code code() const {
        if (state_ == nullptr) {
            return kOk;
        }
        return reinterpret_cast<const Header*>(state_.get())->code_;
    }

    std::string message() const;

private:
    // REQUIRES: stat_ != nullptr
    uint16_t size() const {
        return reinterpret_cast<const Header*>(state_.get())->size_;
    }

    Status(Code code, folly::StringPiece msg);

    static std::unique_ptr<const char[]> copyState(const char *state);

    static std::string format(const char *fmt, va_list args);

    static const char *toString(Code code);

private:
    struct Header {
        uint16_t                    size_;
        Code                        code_;
    };
    static constexpr auto kHeaderSize = sizeof(Header);
    // state_ == nullptr indicates OK
    // otherwise, the buffer layout is:
    // state_[0..1]                 length of the error msg, i.e. size() - kHeaderSize
    // state_[2..3]                 code
    // state_[4...]                 verbose error message
    std::unique_ptr<const char[]>   state_;
};


inline std::ostream& operator<<(std::ostream &os, const Status &status) {
    return os << status.toString();
}

}   // namespace nebula

#define NG_RETURN_IF_ERROR(s)                                                                      \
    do {                                                                                           \
        const auto &__s = (s);                                                                     \
        if (UNLIKELY(!__s.ok())) {                                                                 \
            return ::nebula::Status::from(__s);                                                    \
        }                                                                                          \
    } while (0)

#define NG_LOG_AND_RETURN_IF_ERROR(s)                                                              \
    do {                                                                                           \
        const auto &__s = (s);                                                                     \
        if (UNLIKELY(!__s.ok())) {                                                                 \
            ::nebula::Status __status = ::nebula::Status::from(__s);                               \
            LOG(ERROR) << __status;                                                                \
            return __status;                                                                       \
        }                                                                                          \
    } while (0)

#endif  // COMMON_BASE_STATUS_H_
