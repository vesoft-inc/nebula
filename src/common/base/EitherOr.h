/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_EITHEROR_H_
#define COMMON_BASE_EITHEROR_H_

#include "common/base/Base.h"
#include <type_traits>

namespace nebula {

using LeftType = std::true_type;
using RightType = std::false_type;

enum class State : int16_t {
    VOID = 0,
    LEFT_TYPE = 1,
    RIGHT_TYPE = 2,
};

static constexpr LeftType* kConstructLeft = nullptr;
static constexpr RightType* kConstructRight = nullptr;


/**
 * EitherOr<> is a type to hold a value of either LEFT type or RIGHT type.
 * The only constrain is the LEFT type cannot be same as the RIGHT type
 *
 * vs. boost::variant<>: Variant is an old design which tries to solve the
 * problem that only POD can be allowed in `union`
 * (https://www.boost.org/doc/libs/1_69_0/doc/html/variant.html#variant.abstract).
 * The problem boost:::variant tries to solve does not exist any more since C++11
 * (https://en.cppreference.com/w/cpp/language/union). EitherOr is designed based
 * on the new `union` in C++11, so it means they are more efficient
 *
 * vs. StatusOr<>: EitherOr is a more generic form of StatusOr. EitherOr supports
 * any two types, as long as the two types are different. StatusOr supports Status
 * and another type. So StatusOr is a special case of EitherOr. Later we would
 * rewrite StatusOr based on EitherOr
 *
 * EitherOr<> provides multiple constructors. Besides the default constructor
 * and copy constructors, EitherOr<> will use the parameters to decide which
 * type should be instantiated. If the LEFT and RIGHT types are similar enough
 * and EitherOr<> cannot decide which type to instantiate, we need to use the
 * constructors with the type Tag. For example
 *
 *   EitherOr<int32_t, uint32_t> v1(kConstructLeft, 12);
 *   EXPECT_TRUE(v1.isLeftType());
 *
 *   EitherOr<int32_t, uint32_t> v1(kConstructRight, 12);
 *   EXPECT_TRUE(v1.isRightType());
 */
template<typename LEFT, typename RIGHT>
class EitherOr {
private:
    // Make friends with other compatible EitherOr<U, V>
    template <typename U, typename V>
    friend class EitherOr;

    static_assert(!std::is_same<LEFT, RIGHT>::value,
                  "The left type and right type of EitherOr<> cannot be same");

    template<class... Args>
    struct TypeConverter {
        TypeConverter() = delete;

        template<class... Types>
        static constexpr typename std::enable_if_t<
            std::is_constructible<LEFT, Types...>::value &&
                !std::is_constructible<RIGHT, Types...>::value,
            LeftType
        >* sfinae() { return kConstructLeft; }

        template<class... Types>
        static constexpr typename std::enable_if_t<
            !std::is_constructible<LEFT, Types...>::value &&
                std::is_constructible<RIGHT, Types...>::value,
            RightType
        >* sfinae() { return kConstructRight; }

        template<class... Types>
        static constexpr typename std::enable_if_t<
            std::is_constructible<LEFT, Types...>::value &&
                std::is_constructible<RIGHT, Types...>::value
        >* sfinae() {
            static_assert(std::is_constructible<LEFT, Types...>::value &&
                            std::is_constructible<RIGHT, Types...>::value,
                          "The arguments can be converted into either"
                          " LEFT or RIGHT, so please use the constructor"
                          " with an explicit tag");
        }

        static constexpr State getState() {
            auto* ptr = sfinae<Args...>();
            if (std::is_same<decltype(ptr), LeftType*>::value) {
                return State::LEFT_TYPE;
            }
            if (std::is_same<decltype(ptr), RightType*>::value) {
                return State::RIGHT_TYPE;
            }
            return State::VOID;
        }

        static constexpr auto* type = sfinae<Args...>();
        static constexpr State state = getState();
    };

    template<class... Args>
    static constexpr decltype(TypeConverter<Args...>::type) convert_to_t{};

    template<class... Args>
    static constexpr State convert_to_s = TypeConverter<Args...>::state;

public:
    virtual ~EitherOr() {
        destruct();
    }

    /***********************************************
     *
     * Constructors
     *
     **********************************************/
    EitherOr() noexcept {};

    EitherOr(const EitherOr& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, rhs.val_.left_);
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, rhs.val_.right_);
                state_ = State::RIGHT_TYPE;
                break;
        }
    }

    EitherOr(EitherOr&& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, std::move(rhs).left());
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, std::move(rhs).right());
                state_ = State::RIGHT_TYPE;
                break;
        }
    }

    template<
        typename U, typename V,
        typename = std::enable_if_t<
            std::is_constructible<LEFT, U>::value &&
            std::is_constructible<RIGHT, V>::value
        >
    >
    EitherOr(const EitherOr<U, V>& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, rhs.val_.left_);
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, rhs.val_.right_);
                state_ = State::RIGHT_TYPE;
                break;
        }
    }

    template<
        typename U, typename V,
        typename = std::enable_if_t<
            std::is_constructible<LEFT, U>::value &&
            std::is_constructible<RIGHT, V>::value
        >
    >
    EitherOr(EitherOr<U, V>&& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, std::move(rhs).left());
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, std::move(rhs).right());
                state_ = State::RIGHT_TYPE;
                break;
        }
    }

    EitherOr(const LEFT& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructLeft, v);
        state_ = State::LEFT_TYPE;
    }

    EitherOr(LEFT&& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructLeft, std::move(v));
        state_ = State::LEFT_TYPE;
    }

    EitherOr(const RIGHT& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructRight, v);
        state_ = State::RIGHT_TYPE;
    }

    EitherOr(RIGHT&& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructRight, std::move(v));
        state_ = State::RIGHT_TYPE;
    }

    // Construct from a list of values which can only construct either
    // LEFT or RIGHT, not both
    template<class... Args,
             typename = std::enable_if_t<
                std::is_constructible<LEFT, Args...>::value ||
                std::is_constructible<RIGHT, Args...>::value>
    >
    EitherOr(Args&&... v) noexcept {  // NOLINT
        new (&val_) Variant(convert_to_t<Args...>, std::forward<Args>(v)...);
        state_ = convert_to_s<Args...>;
    }

    // Construct from a value which can construct both LEFT and RIGHT
    // So we use a type tag to force selecting LEFT
    template<typename U,
             typename = std::enable_if_t<
                std::is_constructible<LEFT, U>::value &&
                std::is_constructible<RIGHT, U>::value
             >
    >
    EitherOr(const LeftType*, U&& v) noexcept {
        new (&val_) Variant(kConstructLeft, std::forward<U>(v));
        state_ = State::LEFT_TYPE;
    }

    // Construct from a value which can construct both LEFT and RIGHT
    // So we use a type tag to force selecting RIGHT
    template<typename U,
             typename = std::enable_if_t<
                std::is_constructible<LEFT, U>::value &&
                std::is_constructible<RIGHT, U>::value
             >
    >
    EitherOr(const RightType*, U&& v) noexcept {
        new (&val_) Variant(kConstructRight, std::forward<U>(v));
        state_ = State::RIGHT_TYPE;
    }


    /***********************************************
     *
     * Assignments
     *
     **********************************************/
    EitherOr& operator=(const LEFT& v) noexcept {
        reset();
        new (&val_) Variant(kConstructLeft, v);
        state_ = State::LEFT_TYPE;
        return *this;
    }

    EitherOr& operator=(LEFT&& v) noexcept {
        reset();
        new (&val_) Variant(kConstructLeft, std::move(v));
        state_ = State::LEFT_TYPE;
        return *this;
    }

    EitherOr& operator=(const RIGHT& v) noexcept {
        reset();
        new (&val_) Variant(kConstructRight, v);
        state_ = State::RIGHT_TYPE;
        return *this;
    }

    EitherOr& operator=(RIGHT&& v) noexcept {
        reset();
        new (&val_) Variant(kConstructRight, std::move(v));
        state_ = State::RIGHT_TYPE;
        return *this;
    }

    // Assign from a value which can only construct either LEFT or RIGHT,
    // but not both
    template<typename U>
    typename std::enable_if_t<
        std::is_constructible<LEFT, U>::value || std::is_constructible<RIGHT, U>::value,
        EitherOr
    >&
    operator=(U&& v) noexcept {
        reset();
        new (&val_) Variant(convert_to_t<U>, std::forward<U>(v));
        state_ = convert_to_s<U>;
        return *this;
    }

    EitherOr& operator=(const EitherOr& rhs) noexcept {
        // Avoid self-assignment
        if (&rhs == this) {
            return *this;
        }

        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, rhs.left());
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, rhs.right());
                state_ = State::RIGHT_TYPE;
                break;
        }
        return *this;
    }

    EitherOr& operator=(EitherOr&& rhs) noexcept {
        // Avoid self-assignment
        if (&rhs == this) {
            return *this;
        }

        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, std::move(rhs).left());
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, std::move(rhs).right());
                state_ = State::RIGHT_TYPE;
                break;
        }
        return *this;
    }

    template<typename U, typename V>
    typename std::enable_if_t<
        std::is_constructible<LEFT, U>::value && std::is_constructible<RIGHT, V>::value,
        EitherOr
    >& operator=(const EitherOr<U, V>& rhs) noexcept {
        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, rhs.left());
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, rhs.right());
                state_ = State::RIGHT_TYPE;
                break;
        }
        return *this;
    }

    template<typename U, typename V>
    typename std::enable_if_t<
        std::is_constructible<LEFT, U>::value && std::is_constructible<RIGHT, V>::value,
        EitherOr
    >& operator=(EitherOr<U, V>&& rhs) noexcept {
        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::LEFT_TYPE:
                new (&val_) Variant(kConstructLeft, std::move(rhs).left());
                state_ = State::LEFT_TYPE;
                break;
            case State::RIGHT_TYPE:
                new (&val_) Variant(kConstructRight, std::move(rhs).right());
                state_ = State::RIGHT_TYPE;
                break;
        }
        return *this;
    }

    /***********************************************
     *
     * State check
     *
     **********************************************/
    bool isVoid() const {
        return state_ == State::VOID;
    }

    bool isLeftType() const {
        return state_ == State::LEFT_TYPE;
    }

    bool isRightType() const {
        return state_ == State::RIGHT_TYPE;
    }

    /***********************************************
     *
     * Access values
     *
     **********************************************/
    LEFT& left() & {
        CHECK(isLeftType());
        return val_.left_;
    }

    const LEFT& left() const & {
        CHECK(isLeftType());
        return val_.left_;
    }

    LEFT left() && {
        CHECK(isLeftType());
        auto v = std::move(val_.left_);
        val_.left_.~LEFT();
        state_ = State::VOID;
        return v;
    }

    RIGHT& right() & {
        CHECK(isRightType());
        return val_.right_;
    }

    const RIGHT& right() const & {
        CHECK(isRightType());
        return val_.right_;
    }

    RIGHT right() && {
        CHECK(isRightType());
        auto v = std::move(val_.right_);
        val_.right_.~RIGHT();
        state_ = State::VOID;
        return v;
    }

private:
    void destruct() {
        switch (state_) {
            case State::VOID:
                return;
            case State::LEFT_TYPE:
                val_.left_.~LEFT();
                return;
            case State::RIGHT_TYPE:
                val_.right_.~RIGHT();
        }
    }

    void reset() {
        destruct();
        state_ = State::VOID;
    }

private:
    union Variant {
        Variant() {}

        template<class... Args>
        Variant(const LeftType*, Args&&... v) : left_(std::forward<Args>(v)...) {}

        template<class... Args>
        Variant(const RightType*, Args&&... v) : right_(std::forward<Args>(v)...) {}

        ~Variant() {}

        LEFT left_;
        RIGHT right_;
    };

    Variant val_;
    State state_{State::VOID};
};

}  // namespace nebula
#endif  // COMMON_BASE_EITHEROR_H_
