/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_BASE_MURMURHASH2_H_
#define COMMON_BASE_MURMURHASH2_H_

#include <string>
#include <cstring>
#include <memory>
#include <thread>
#include <type_traits>

namespace nebula {

/**
 * This is an implementation of MurmurHash2,
 * which is identical to `std::hash'(at least until GCC 8.1).
 * This one is more performant on short strings, because:
 *  1. It could be inlined.
 *  2. It utilizes the loop unrolling trick.
 * Besides, it works with the plain old raw bytes array!
 */
class MurmurHash2 {
    template <typename T>
    static constexpr bool is_char_v = std::is_same<T, char>::value ||
                                      std::is_same<T, signed char>::value ||
                                      std::is_same<T, unsigned char>::value;

public:
    // std::string
    size_t operator()(const std::string &str) const noexcept {
        return this->operator()(str.data(), str.length());
    }

    // null-terminated C-style string
    template <typename T, typename = std::enable_if_t<is_char_v<T>>>
    size_t operator()(const T *&str) const noexcept {
        return this->operator()(str, ::strlen(str));
    }

    // raw bytes array
    template <typename T, typename = std::enable_if_t<is_char_v<T>>>
    size_t operator()(const T *str, size_t size) const noexcept {
        uint64_t seed = 0xc70f6907UL;
        const uint64_t m = 0xc6a4a7935bd1e995;
        const uint32_t r = 47;
        uint64_t h = seed ^ (size * m);
        const uint64_t *data = (const uint64_t *)str;
        const uint64_t *end = data + (size / 8);
        while (data != end) {
            uint64_t k = *data++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        const unsigned char *data2 = (const unsigned char*)data;
        switch (size & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;  // fallthrough
        case 6:
            h ^= uint64_t(data2[5]) << 40;  // fallthrough
        case 5:
            h ^= uint64_t(data2[4]) << 32;  // fallthrough
        case 4:
            h ^= uint64_t(data2[3]) << 24;  // fallthrough
        case 3:
            h ^= uint64_t(data2[2]) << 16;  // fallthrough
        case 2:
            h ^= uint64_t(data2[1]) << 8;   // fallthrough
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;     // fallthrough
        }
        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }

    // std::thread::id
    size_t operator()(std::thread::id id) const noexcept {
        return std::hash<std::thread::id>()(id);
    }

    // literal string(without decay)
    template <size_t N, typename T, typename = std::enable_if_t<is_char_v<T>>>
    size_t operator()(const T (&str)[N]) const noexcept {
        return this->operator()(str, N - 1);
    }

    // integer
    template <typename T>
    std::enable_if_t<std::is_integral<T>::value, size_t>
    operator()(T key) const noexcept {
        return static_cast<size_t>(key);
    }

    // pointers
    template <typename T>
    std::enable_if_t<!is_char_v<T>, size_t>
    operator() (const T *ptr) const noexcept {
        return reinterpret_cast<size_t>(ptr);
    }

    // std::shared_ptr
    template <typename T>
    size_t operator()(const std::shared_ptr<T> &ptr) const noexcept {
        return reinterpret_cast<size_t>(ptr.get());
    }

    // std::unique_ptr
    template <typename T, typename Deleter>
    size_t operator()(const std::unique_ptr<T, Deleter> &ptr) const noexcept {
        return reinterpret_cast<size_t>(ptr.get());
    }
};

}   // namespace nebula

#endif  // COMMON_BASE_MURMURHASH2_H_
