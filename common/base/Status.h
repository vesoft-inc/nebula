/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_BASE_STATUS_H_
#define COMMON_BASE_STATUS_H_

#include "base/Base.h"

/**
 * Status is modeled on the one from levelDB, beyond that,
 * this one adds support on move semantics and formated error messages.
 *
 * Status is as cheap as raw pointers in the successfull case,
 * without any heap memory allocations.
 */

namespace vesoft {

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

    bool operator==(const Status &rhs) const {
        // `state_ == rhs.state_' means either `this == &rhs',
        // or both `*this' and `rhs' are OK
        if (state_ == rhs.state_) {
            return true;
        }
        return code() == rhs.code();
    }

    bool ok() const {
        return state_ == nullptr;
    }

    static Status OK() {
        return Status();
    }

    static Status VoidStatusOr() {
        return Status(kVoidStatusOr, "StatusOr contains neither Status nor value");
    }

#define STATUS_GENGERATOR(ERROR)                        \
    static Status ERROR(folly::StringPiece msg) {       \
        return Status(k##ERROR, msg);                   \
    }                                                   \
                                                        \
    static Status ERROR(const char *fmt, ...) {         \
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

    STATUS_GENGERATOR(Error);
    STATUS_GENGERATOR(SyntaxError);
    STATUS_GENGERATOR(NotSupported);

#undef STATUS_GENGERATOR

    std::string toString() const;

    friend std::ostream& operator<<(std::ostream &os, const Status &status);

private:
    // If some kind of error really needs to be distinguished with others using a specific
    // code, other than a general code and specific msg, you could add a new code below,
    // e.g. kSomeError, and add the cooresponding STATUS_GENERATOR(SomeError)
    enum Code : uint16_t {
        // OK
        kOk                     = 0,
        // 10xx, for general errors
        kError                  = 101,
        kVoidStatusOr           = 102,
        kNotSupported           = 103,
        // 2xx, for graph engine errors
        kSyntaxError            = 201,
        // 3xx, for storage engine errors
        // ...
        // 4xx, for meta service errors
        // ...
    };

    Code code() const {
        if (state_ == nullptr) {
            return kOk;
        }
        return reinterpret_cast<const Header*>(state_.get())->code_;
    }
    // REQUIRES: stat_ != nullptr
    uint16_t size() const {
        return reinterpret_cast<const Header*>(state_.get())->size_;
    }

    Status(Code code, folly::StringPiece msg);

    static std::unique_ptr<const char[]> copyState(const char *state);

    static std::string format(const char *fmt, va_list args);

private:
    struct Header {
        uint16_t                    size_;
        Code                        code_;
    };
    static constexpr auto kHeaderSize = 4;
    static_assert(sizeof(Header) == kHeaderSize, "");
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

}   // namespace vesoft

#endif  // COMMON_BASE_STATUS_H_
