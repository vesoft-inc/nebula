/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_CORD_H_
#define COMMON_BASE_CORD_H_

#include <stdlib.h>
#include <functional>
#include <string>

namespace nebula {

class Cord {
public:
    Cord() = default;
    explicit Cord(int32_t blockSize);
    virtual ~Cord();

    size_t size() const noexcept;
    bool empty() const noexcept;

    // Apply each block to the visitor until the end or the visitor
    // returns false
    bool applyTo(std::function<bool(const char*, int32_t)> visitor) const;

    // Append the cord content to the given string
    size_t appendTo(std::string& str) const;
    // Convert the cord content to a new string
    std::string str() const;

    void clear();

    Cord& write(const char* value, size_t len);

    Cord& operator<<(int8_t value);
    Cord& operator<<(uint8_t value);

    Cord& operator<<(int16_t value);
    Cord& operator<<(uint16_t value);

    Cord& operator<<(int32_t value);
    Cord& operator<<(uint32_t value);

    Cord& operator<<(int64_t value);
    Cord& operator<<(uint64_t value);

    Cord& operator<<(char value);
    Cord& operator<<(bool value);

    Cord& operator<<(float value);
    Cord& operator<<(double value);

    Cord& operator<<(const std::string& value);
    Cord& operator<<(const char* value);

    Cord& operator<<(const Cord& rhs);

private:
    const int32_t blockSize_ = 1024;
    const int32_t blockContentSize_ = 1024 - sizeof(char*);
    int32_t blockPt_ = blockContentSize_;
    size_t len_ = 0;

    char* head_ = nullptr;
    char* tail_ = nullptr;

    void allocateBlock();
};

}  // namespace nebula
#endif  // COMMON_BASE_CORD_H_

