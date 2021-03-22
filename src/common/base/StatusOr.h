/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_STATUSOR_H_
#define COMMON_BASE_STATUSOR_H_

#include "common/base/Base.h"
#include "common/base/Status.h"

namespace nebula {

template <typename T>
class StatusOr final {
public:
    // Make friends with other compatible StatusOr<U>
    template <typename U>
    friend class StatusOr;

    // Convenience type traits
    template <typename U>
    static constexpr bool is_status_v = std::is_same<Status, std::decay_t<U>>::value;

    // Tell if `U' is of type `StatusOr<V>'
    template <typename U>
    struct is_status_or {
        static auto sfinae(...) -> uint8_t { return 0; }
        template <typename V>
        static auto sfinae(const StatusOr<V>&) -> uint16_t { return 0; }
        static constexpr auto value = (sizeof(sfinae(std::declval<U>())) == sizeof(uint16_t));
    };

    template <typename U>
    static constexpr bool is_status_or_v = is_status_or<U>::value;

    // Tell if `T' is initializable from `U'.
    // We simply use `is_constructible_v' for now,
    // because we are only utilizing direct-initializations.
    // Besides, we must exclude `Status' and `StatusOr<V>' from `U'.
    //
    // TODO(dutor) we may take other cases into account in future,
    // e.g. convertible but not constructible.
    template <typename U>
    static constexpr bool is_initializable_v = is_constructible_v<T, U> &&
                                               std::is_convertible<U, T>::value &&
                                               !is_status_or_v<U> &&
                                               !is_status_v<U>;

    // Assert that `T' must be copy/move constructible
    static_assert(is_copy_or_move_constructible_v<T>, "`T' must be copy/move constructible");

    // Assert that `T' must not be of type reference
    static_assert(!std::is_reference<T>::value, "`T' must not be of type reference");

    // Assert that `T' must not be of type `Status'
    static_assert(!is_status_v<T>, "`T' must not be of type `Status'");

    // Assert that `T' must not be of type `StatusOr'
    static_assert(!is_status_or_v<T>, "`T' must not be of type `StatusOr'");

    // `StatusOr<T>' contains neither a Status nor a value
    // in the default-constructed case.
    // From the semantics aspect, it must have been associated with
    // a Status or value eventualy before being used.
    StatusOr() {
        state_ = kVoid;
    }

    // Destruct the `Status' or value if it's holding one.
    ~StatusOr() {
        destruct();
    }

    // Copy/move construct from `Status'
    // Not explicit to allow construct from a `Status', e.g. in the `return' statement
    template <typename U>
    StatusOr(U &&status, std::enable_if_t<is_status_v<U>>* = nullptr)   // NOLINT
        : variant_(std::forward<U>(status)) {
        state_ = kStatus;
    }

    // Copy/move construct with a value of any compatible type
    // Not explicit to allow construct from a value, e.g. in the `return' statement
    template <typename U, typename = std::enable_if_t<is_initializable_v<U>>>
    StatusOr(U &&value)     // NOLINT
        : variant_(std::forward<U>(value)) {
        state_ = kValue;
    }

    StatusOr(T &&value)     // NOLINT
        : variant_(std::move(value)) {
        state_ = kValue;
    }

    // Copy constructor
    StatusOr(const StatusOr &rhs) : state_(rhs.state_) {
        if (hasValue()) {
            new (&variant_) Variant(rhs.variant_.value_);
        } else if (hasStatus()) {
            new (&variant_) Variant(rhs.variant_.status_);
        }
    }

    // Copy construct from a lvalue of `StatusOr<U>'
    template <typename U, typename = std::enable_if_t<is_initializable_v<U>>>
    StatusOr(const StatusOr<U> &rhs) : state_(rhs.state_) {
        if (hasValue()) {
            new (&variant_) Variant(rhs.variant_.value_);
        } else if (hasStatus()) {
            new (&variant_) Variant(rhs.variant_.status_);
        }
    }

    // Copy assignment operator
    StatusOr& operator=(const StatusOr &rhs) {
        if (&rhs == this) {
            return *this;
        }
        reset();
        if (rhs.hasValue()) {
            new (&variant_) Variant(rhs.variant_.value_);
            state_ = kValue;
        } else if (rhs.hasStatus()) {
            new (&variant_) Variant(rhs.variant_.status_);
            state_ = kStatus;
        }
        return *this;
    }

    // Move constructor
    StatusOr(StatusOr &&rhs) noexcept : state_(rhs.state_) {
        if (hasValue()) {
            new (&variant_) Variant(std::move(rhs.variant_.value_));
            rhs.resetValue();
        } else if (rhs.hasStatus()) {
            new (&variant_) Variant(std::move(rhs.variant_.status_));
            rhs.resetStatus();
        }
    }

    // Move construct from a rvalue of StatusOr<U>
    template <typename U, typename = std::enable_if_t<is_initializable_v<U>>>
    StatusOr(StatusOr<U> &&rhs) noexcept : state_(rhs.state_) {
        if (hasValue()) {
            new (&variant_) Variant(std::move(rhs.variant_.value_));
            rhs.resetValue();
        } else if (rhs.hasStatus()) {
            new (&variant_) Variant(std::move(rhs.variant_.status_));
            rhs.resetStatus();
        } else {
            // do nothing on void
        }
    }

    // Move assignment operator
    StatusOr& operator=(StatusOr &&rhs) noexcept {
        if (&rhs == this) {
            return *this;
        }
        reset();
        if (rhs.hasValue()) {
            new (&variant_) Variant(std::move(rhs.variant_.value_));
            rhs.resetValue();
            state_ = kValue;
        } else if (rhs.hasStatus()) {
            new (&variant_) Variant(std::move(rhs.variant_.status_));
            rhs.resetStatus();
            state_ = kStatus;
        } else {
            // do nothing on void, since it's already reset
        }
        return *this;
    }

    // Move assigment operator from a rvalue of `StatusOr<U>'
    template <typename U, typename = std::enable_if_t<is_initializable_v<U>>>
    StatusOr& operator=(StatusOr<U> &&rhs) noexcept {
        reset();
        if (rhs.hasValue()) {
            new (&variant_) Variant(std::move(rhs.variant_.value_));
            rhs.resetValue();
            state_ = kValue;
        } else if (rhs.hasStatus()) {
            new (&variant_) Variant(std::move(rhs.variant_.status_));
            rhs.resetStatus();
            state_ = kStatus;
        } else {
            // do nothing on void, since it's already reset
        }
        return *this;
    }

    // Move assigment operator from a rvalue of any compatible type with `T'
    template <typename U, typename = std::enable_if_t<is_initializable_v<U>>>
    StatusOr& operator=(U &&value) noexcept {
        destruct();
        new (&variant_) Variant(std::forward<U>(value));
        state_ = kValue;
        return *this;
    }

    // Copy assign from a lvalue of `Status'
    StatusOr& operator=(const Status &status) {
        destruct();
        new (&variant_) Variant(status);
        state_ = kStatus;
        return *this;
    }

    // Move assign from a rvalue of `Status'
    StatusOr& operator=(Status &&status) noexcept {
        destruct();
        new (&variant_) Variant(std::move(status));
        state_ = kStatus;
        return *this;
    }

    // Tell if `*this' contains a value
    bool ok() const {
        return hasValue();
    }

    // Type conversion operator, i.e. alias of `ok()'
    explicit operator bool() const {
        return ok();
    }

    // Return the associated `Status' if and only if it has one,
    //
    Status status() const & {
        CHECK(hasStatus());
        return variant_.status_;
    }

    Status status() && {
        CHECK(hasStatus());
        auto status = std::move(variant_.status_);
        resetStatus();
        return status;
    }

    // Return the non-const lvalue reference to the associated value
    // `ok()' is DCHECKed
    T& value() & {
        DCHECK(ok());
        return variant_.value_;
    }

    // Return the const lvalue reference to the associated value
    const T& value() const & {
        DCHECK(ok());
        return variant_.value_;
    }

    // Move the associated value out.
    // `*this' must be a rvalue
    T value() && {
        DCHECK(ok());
        auto value = std::move(variant_.value_);
        resetValue();
        return value;
    }

private:
    bool hasValue() const {
        return state_ == kValue;
    }

    bool hasStatus() const {
        return state_ == kStatus;
    }

    bool isVoid() const {
        return state_ == kVoid;
    }

    void destructValue() {
        variant_.value_.~T();
    }

    void destructStatus() {
        variant_.status_.~Status();
    }

    void resetValue() {
        destructValue();
        state_ = kVoid;
    }

    void resetStatus() {
        destructStatus();
        state_ = kVoid;
    }

    void destruct() {
        if (isVoid()) {
            return;
        }
        if (hasValue()) {
            destructValue();
        } else {
            destructStatus();
        }
    }

    void reset() {
        destruct();
        state_ = kVoid;
    }

private:
    // Variant type to hold a `Status', or a `T', or nothing
    union Variant {
        Variant() {}
        Variant(const Status &status) : status_(status) {}
        Variant(Status &&status) : status_(std::move(status)) {}
        template <typename U, typename = std::enable_if_t<is_initializable_v<U>>>
        Variant(U &&value) : value_(std::forward<U>(value)) {}
        ~Variant() {}

        Status      status_;
        T           value_;
    };

    static constexpr uint8_t kVoid = 0;
    static constexpr uint8_t kStatus = 1;
    static constexpr uint8_t kValue = 2;

    Variant         variant_;
    uint8_t         state_;
};

}   // namespace nebula

#endif  // COMMON_BASE_STATUSOR_H_
