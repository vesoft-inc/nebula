/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_BASE_EITHEROR_H_
#define COMMON_BASE_EITHEROR_H_

#include "base/Base.h"
#include <type_traits>

namespace nebula {

namespace {  // NOLINT

template<typename U>
struct TypeSelector {
    TypeSelector() = delete;
};

using TypeOne = TypeSelector<char*>;
using TypeTwo = TypeSelector<float>;

enum class State : int16_t {
    VOID = 0,
    FIRST_TYPE = 1,
    SECOND_TYPE = 2,
};

}  // Anonymous namespace

static constexpr TypeOne* kConstructT1 = nullptr;
static constexpr TypeTwo* kConstructT2 = nullptr;


template<typename T1,
         typename T2,
         typename = std::enable_if_t<!std::is_same<T1, T2>::value>
>
class EitherOr {
private:
    // Make friends with other compatible StatusOr<U>
    template <typename U, typename V, typename Cond>
    friend class EitherOr;

    template<class... Args>
    struct TypeConverter {
        TypeConverter() = delete;

        template<class... Types>
        static constexpr typename std::enable_if_t<
            std::is_constructible<T1, Types...>::value &&
                !std::is_constructible<T2, Types...>::value,
            TypeOne
        >* sfinae() { return nullptr; }

        template<class... Types>
        static constexpr typename std::enable_if_t<
            !std::is_constructible<T1, Types...>::value &&
                std::is_constructible<T2, Types...>::value,
            TypeTwo
        >* sfinae() { return nullptr; }

        template<class... Types>
        static constexpr typename std::enable_if_t<
            std::is_constructible<T1, Types...>::value &&
                std::is_constructible<T2, Types...>::value
        > sfinae() {
            static_assert(std::is_constructible<T1, Types...>::value &&
                            std::is_constructible<T2, Types...>::value,
                          "The arguments can be converted into either"
                          " T1 or T2, so please use the construct or"
                          " with explicit tag");
        }

        static constexpr State getState() {
            auto ptr = sfinae<Args...>();
            if (std::is_same<decltype(ptr), TypeOne*>::value) {
                return State::FIRST_TYPE;
            }
            if (std::is_same<decltype(ptr), TypeTwo*>::value) {
                return State::SECOND_TYPE;
            }
            return State::VOID;
        }

        static constexpr auto* type = sfinae<Args...>();
        static constexpr State state = getState();
    };

    template<class... Args>
    static constexpr auto convert_to_t = TypeConverter<Args...>::type;

    template<class... Args>
    static constexpr auto convert_to_s = TypeConverter<Args...>::state;

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
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, rhs.val_.v1_);
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, rhs.val_.v2_);
                state_ = State::SECOND_TYPE;
                break;
        }
    }

    EitherOr(EitherOr&& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, std::move(rhs).v1());
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, std::move(rhs).v2());
                state_ = State::SECOND_TYPE;
                break;
        }
    }

    template<
        typename U, typename V,
        typename = std::enable_if_t<
            std::is_constructible<T1, U>::value &&
            std::is_constructible<T2, V>::value
        >
    >
    EitherOr(const EitherOr<U, V>& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, rhs.val_.v1_);
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, rhs.val_.v2_);
                state_ = State::SECOND_TYPE;
                break;
        }
    }

    template<
        typename U, typename V,
        typename = std::enable_if_t<
            std::is_constructible<T1, U>::value &&
            std::is_constructible<T2, V>::value
        >
    >
    EitherOr(EitherOr<U, V>&& rhs) noexcept {
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, std::move(rhs).v1());
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, std::move(rhs).v2());
                state_ = State::SECOND_TYPE;
                break;
        }
    }

    EitherOr(const T1& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructT1, v);
        state_ = State::FIRST_TYPE;
    }

    EitherOr(T1&& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructT1, std::move(v));
        state_ = State::FIRST_TYPE;
    }

    EitherOr(const T2& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructT2, v);
        state_ = State::SECOND_TYPE;
    }

    EitherOr(T2&& v) noexcept {  // NOLINT
        new (&val_) Variant(kConstructT2, std::move(v));
        state_ = State::SECOND_TYPE;
    }

    // Construct from a list of values which can only construct either
    // T1 or T2, not both
    template<class... Args,
             typename = std::enable_if_t<
                std::is_constructible<T1, Args...>::value ||
                std::is_constructible<T2, Args...>::value>
    >
    EitherOr(Args&&... v) noexcept {  // NOLINT
        new (&val_) Variant(convert_to_t<Args...>, std::forward<Args>(v)...);
        state_ = convert_to_s<Args...>;
    }

    // Construct from a value which can construct both T1 and T2
    // So we use a type tag to force selecting T1
    template<typename U,
             typename = std::enable_if_t<
                std::is_constructible<T1, U>::value &&
                std::is_constructible<T2, U>::value
             >
    >
    EitherOr(const TypeOne*, U&& v) noexcept {
        new (&val_) Variant(kConstructT1, std::forward<U>(v));
        state_ = State::FIRST_TYPE;
    }

    // Construct from a value which can construct both T1 and T2
    // So we use a type tag to force selecting T2
    template<typename U,
             typename = std::enable_if_t<
                std::is_constructible<T1, U>::value &&
                std::is_constructible<T2, U>::value
             >
    >
    EitherOr(const TypeTwo*, U&& v) noexcept {
        new (&val_) Variant(kConstructT2, std::forward<U>(v));
        state_ = State::SECOND_TYPE;
    }


    /***********************************************
     *
     * Assignments
     *
     **********************************************/
    EitherOr& operator=(const T1& v) noexcept {
        reset();
        new (&val_) Variant(kConstructT1, v);
        state_ = State::FIRST_TYPE;
        return *this;
    }

    EitherOr& operator=(T1&& v) noexcept {
        reset();
        new (&val_) Variant(kConstructT1, std::move(v));
        state_ = State::FIRST_TYPE;
        return *this;
    }

    EitherOr& operator=(const T2& v) noexcept {
        reset();
        new (&val_) Variant(kConstructT2, v);
        state_ = State::SECOND_TYPE;
        return *this;
    }

    EitherOr& operator=(T2&& v) noexcept {
        reset();
        new (&val_) Variant(kConstructT2, std::move(v));
        state_ = State::SECOND_TYPE;
        return *this;
    }

    // Assign from a value which can only construct either T1 or T2,
    // but not both
    template<typename U>
    typename std::enable_if_t<
        std::is_constructible<T1, U>::value || std::is_constructible<T2, U>::value,
        EitherOr
    >&
    operator=(U&& v) noexcept {
        reset();
        new (&val_) Variant(convert_to_t<U>, std::forward<U>(v));
        state_ = convert_to_s<U>;
        return *this;
    }

    EitherOr& operator=(const EitherOr& rhs) noexcept {
        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, rhs.v1());
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, rhs.v2());
                state_ = State::SECOND_TYPE;
                break;
        }
        return *this;
    }

    EitherOr& operator=(EitherOr&& rhs) noexcept {
        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, std::move(rhs).v1());
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, std::move(rhs).v2());
                state_ = State::SECOND_TYPE;
                break;
        }
        return *this;
    }

    template<typename U, typename V>
    typename std::enable_if_t<
        std::is_constructible<T1, U>::value && std::is_constructible<T2, V>::value,
        EitherOr
    >& operator=(const EitherOr<U, V>& rhs) noexcept {
        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, rhs.v1());
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, rhs.v2());
                state_ = State::SECOND_TYPE;
                break;
        }
        return *this;
    }

    template<typename U, typename V>
    typename std::enable_if_t<
        std::is_constructible<T1, U>::value && std::is_constructible<T2, V>::value,
        EitherOr
    >& operator=(EitherOr<U, V>&& rhs) noexcept {
        reset();
        switch (rhs.state_) {
            case State::VOID:
                break;
            case State::FIRST_TYPE:
                new (&val_) Variant(kConstructT1, std::move(rhs).v1());
                state_ = State::FIRST_TYPE;
                break;
            case State::SECOND_TYPE:
                new (&val_) Variant(kConstructT2, std::move(rhs).v2());
                state_ = State::SECOND_TYPE;
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

    bool isTypeOne() const {
        return state_ == State::FIRST_TYPE;
    }

    bool isTypeTwo() const {
        return state_ == State::SECOND_TYPE;
    }

    /***********************************************
     *
     * Access values
     *
     **********************************************/
    T1& v1() & {
        CHECK(isTypeOne());
        return val_.v1_;
    }

    const T1& v1() const & {
        CHECK(isTypeOne());
        return val_.v1_;
    }

    T1 v1() && {
        CHECK(isTypeOne());
        auto v = std::move(val_.v1_);
        val_.v1_.~T1();
        state_ = State::VOID;
        return v;
    }

    T2& v2() & {
        CHECK(isTypeTwo());
        return val_.v2_;
    }

    const T2& v2() const & {
        CHECK(isTypeTwo());
        return val_.v2_;
    }

    T2 v2() && {
        CHECK(isTypeTwo());
        auto v = std::move(val_.v2_);
        val_.v2_.~T2();
        state_ = State::VOID;
        return v;
    }

private:
    void destruct() {
        switch (state_) {
            case State::VOID:
                return;
            case State::FIRST_TYPE:
                val_.v1_.~T1();
                return;
            case State::SECOND_TYPE:
                val_.v2_.~T2();
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
        Variant(const TypeOne*, Args&&... v) : v1_(std::forward<Args>(v)...) {}

        template<class... Args>
        Variant(const TypeTwo*, Args&&... v) : v2_(std::forward<Args>(v)...) {}

        ~Variant() {}

        T1 v1_;
        T2 v2_;
    };

    Variant val_;
    State state_{State::VOID};
};

}  // namespace nebula
#endif  // COMMON_BASE_EITHEROR_H_

