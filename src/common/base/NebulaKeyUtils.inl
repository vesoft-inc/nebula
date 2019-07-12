/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


namespace nebula {

template<typename T>
typename std::enable_if<std::is_integral<T>::value, int32_t>::type
NebulaKeyUtils::encodeVersion(T v, uint8_t *encode) {
    size_t len = folly::encodeVarint(v, encode);
    DCHECK_GT(len, 0UL);
    uint8_t temp;
    // Keep the original order
    for (size_t i = 0; i < len / 2; i++) {
        temp = encode[i];
        encode[i] = encode[len-i-1];
        encode[len-i-1] = temp;
    }
    return len;
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value>::type
NebulaKeyUtils::decodeVersion(const char* data, int32_t len, T& v) {
    const uint8_t* encode = reinterpret_cast<const uint8_t*>(data);
    uint8_t copy[10];
    for (auto i = 0; i < len; i++) {
        copy[i] = encode[len-i-1];
    }
    folly::ByteRange range(copy, len);
    v = folly::decodeVarint(range);
}

}  // namespace nebula
