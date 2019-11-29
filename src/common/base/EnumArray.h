/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_ENUMARRAY_H_
#define COMMON_BASE_ENUMARRAY_H_

#include "base/Base.h"

namespace nebula {

template<bool C, typename Then, typename Else>
class _IF {};

template<typename Then, typename Else>
class _IF<true, Then, Else> {
public:
    typedef Then reType;
};

template<typename Then, typename Else>
class _IF<false, Then, Else> {
public:
    typedef Else reType;
};

template<template<class> class Condition, class Statement>
class _WHILE {
    template<typename T>
    class _STOP {
    public:
        typedef T reType;
    };

public:
    typedef typename
        _IF<Condition<Statement>::ret,
        _WHILE<Condition, typename Statement::Next>,
        _STOP<Statement>>::reType::reType
    reType;
};

template<size_t N>
class NextPowerTwo {
    template<typename Stat>
    class Cond {
    public:
        enum {
            ret = (Stat::number != 0),
        };
    };

    template<size_t n, uint8_t cnt>
    class Stat {
    public:
        enum {
            number = n >> 1,
            count = cnt + 1,
        };

        typedef Stat<number, count> Next;
    };

    template<size_t n>
    class isPowerTwo {
    public:
        enum {
            ret = (n != 0) && !(n & (n - 1)),
        };
    };

public:
    enum {
        ret = _IF<isPowerTwo<N>::ret,
                  std::integral_constant<size_t, N>,
                  std::integral_constant<size_t,
                                         1 << _WHILE<Cond, Stat<N, 0>>::reType::count>
                >::reType::value,
    };
};

template<typename E>
class DefaultTransformer {
public:
    static uint64_t toValue(E e) {
        return static_cast<uint64_t>(e);
    }

    static E toEnum(uint8_t v) {
        return static_cast<E>(v);
    }
};

/**
 * Compact enum array, the idea comes from bitset.
 * If the enum has two values, it eauals bitset.
 * */
template<typename E,  // the type of enum
         size_t N,  // total numbers of enums
         // total values number of the E, by default it will read the _ITEMS_NUM field
         uint8_t EnumNum = E::_ITEMS_NUM,
         // Implement toIndex and toValue methods
         typename Transformer = DefaultTransformer<E>,
         typename = std::enable_if_t<std::is_enum<E>::value>>
class EnumArray {
public:
    class EnumArrayIterator: public std::iterator<
                                    std::input_iterator_tag,   // iterator_category
                                    E,                         // value_type
                                    size_t,                    // difference_type
                                    E*,                        // pointer
                                    E                          // reference
                                                 > {
    public:
        explicit EnumArrayIterator(EnumArray<E, N, EnumNum, Transformer>* arr, size_t idx = 0)
            : arr_(arr)
            , idx_(idx) {
            if (idx_ > N) {
                idx_ = N;
            } else if (idx_ < 0) {
                idx_ = 0;
            }
        }

        EnumArrayIterator& operator++() {
            idx_++;
            return *this;
        }

        EnumArrayIterator operator++(int) {
            iterator retval = *this;
            ++(*this);
            return retval;
        }

        EnumArrayIterator operator+(size_t diff) {
            return EnumArrayIterator(arr_, idx_ + diff);
        }

        EnumArrayIterator operator-(size_t diff) {
            return EnumArrayIterator(arr_, idx_ - diff);
        }

        bool operator==(const EnumArrayIterator& other) const {
            return idx_ == other.idx_;
        }

        bool operator!=(const EnumArrayIterator& other) const {
            return !(*this == other);
        }

        E operator*() const {
            return arr_->get(idx_);
        }

    private:
        EnumArray<E, N, EnumNum, Transformer>* arr_ = nullptr;
        size_t idx_ = 0;
    };

    typedef EnumArrayIterator iterator;

    EnumArray() {
        blocks_.fill(0);
    }
    ~EnumArray() = default;

    void put(size_t idx, E e) {
        auto v = Transformer::toValue(e);
        DCHECK(idx < kTotalBits) << "idx " << idx << ", total " << kTotalBits;
        auto bIdx = blockIndex(idx);
        auto offset = bitOffset(idx);
        uint64_t mask =  ((1ul << kLogBitsOfEnum) - 1) << offset;
        blocks_[bIdx] &= (~mask);
        mask = v << offset;
        blocks_[bIdx] |= mask;
        VLOG(3) << "idx " << idx
                << ", v " << static_cast<int32_t>(v)
                << ", offset " << offset
                << ", bIdx " << bIdx
                << std::showbase << std::internal << std::setfill('0') << std::hex
                << ", mask " << std::setw(18) << mask
                << ", block[bIdx] " << std::setw(18) << blocks_[bIdx];
    }

    E get(size_t idx) const {
        DCHECK(idx < kTotalBits);
        auto offset = bitOffset(idx);
        auto bIdx = blockIndex(idx);
        uint64_t mask =  ((1ul << kLogBitsOfEnum) - 1) << offset;
        auto v = (blocks_[bIdx] & mask) >> offset;
        VLOG(3) << "idx " << idx
                << ", offset " << offset
                << ", bIdx " << blockIndex(idx)
                << ", v " << v
                << std::showbase << std::internal << std::setfill('0') << std::hex
                << ", mask " << std::setw(18) << mask
                << ", block[bIdx] " << std::setw(18) << blocks_[bIdx];
        return Transformer::toEnum(v);
    }

    E operator[](size_t idx) const {
        return get(idx);
    }

    iterator begin() {
        return iterator(this, 0);
    }

    iterator end() {
        return iterator(this, N);
    }

    /**
    * Return the size of the bitset.
    */
    constexpr size_t size() const {
        return N;
    }

    constexpr size_t enumNum() const {
        return EnumNum;
    }

private:
    static constexpr size_t blockIndex(size_t bit) {
        return (bit * kLogBitsOfEnum) >> kLogBits;
    }

    static constexpr size_t bitOffset(size_t bit) {
        return (bit * kLogBitsOfEnum) & (kBitsPerBlock - 1);
    }


private:
    static constexpr size_t kNextPowerTwo = NextPowerTwo<EnumNum>::ret;
    static constexpr size_t kLogBitsOfEnum = std::log2(kNextPowerTwo);
    static constexpr size_t kBitsPerBlock = std::numeric_limits<uint64_t>::digits;
    static constexpr uint8_t kLogBits = std::log2(kBitsPerBlock);
    static constexpr size_t kTotalBits = N << kLogBitsOfEnum;
    static constexpr size_t kBlocksNum = kTotalBits % kBitsPerBlock == 0
                                        ? kTotalBits / kBitsPerBlock
                                        : kTotalBits / kBitsPerBlock + 1;

    std::array<uint64_t, kBlocksNum> blocks_;
};

}  // namespace nebula
#endif  // COMMON_BASE_ENUMARRAY_H_
