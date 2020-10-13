/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"

namespace nebula {

template <std::size_t kBlockContentSize = 1024>
class ICord {
public:
    static_assert(kBlockContentSize && !(kBlockContentSize & (kBlockContentSize-1)),
                  "kBlockContentSize must be power of 2");
    ICord() : head_(inlineBlock_), tail_(head_) {}

    virtual ~ICord() {
        clear();
    }

    size_t size() const noexcept {
        return len_;
    }

    bool empty() const noexcept {
        return len_ == 0;
    }

    void clear() {
        auto alloc = next(head_);
        if (alloc) {
            DCHECK(tail_);

            // Need to release all blocks
            char* p = alloc;
            while (p != tail_) {
                char* n = next(p);
                free(p);
                p = n;
            }
            // Free the last block
            free(p);
        }

        len_ = 0;

        head_ = nullptr;
        tail_ = nullptr;
    }

    // Apply each block to the visitor until the end or the visitor
    // returns false
    bool applyTo(std::function<bool(const char*, int32_t)> visitor) const {
        if (empty()) {
            return true;
        }

        char* n = head_;
        while (n != tail_) {
            if (!visitor(n, kBlockContentSize)) {
                // stop visiting further
                return false;
            }
            // Get the pointer to the next block
            n = next(n);
        }

        // Last block
        return visitor(tail_, lengthMod());
    }

    // Append the cord content to the given string
    size_t appendTo(std::string& str) const {
        if (empty()) {
            return 0;
        }

        char* n = head_;
        while (n != tail_) {
            str.append(n, kBlockContentSize);
            // Get the pointer to the next block
            n = next(n);
        }

        // Last block
        std::size_t lengthModSize = lengthMod();
        str.append(tail_, lengthModSize == 0 ? kBlockContentSize : lengthModSize);

        return len_;
    }

    // Convert the cord content to a new string
    std::string str() const {
        std::string buf;
        buf.reserve(len_);
        appendTo(buf);

        return buf;
    }

    ICord<kBlockContentSize>& write(const char* value, size_t len) {
        if (len == 0) {
            return *this;
        }

        std::size_t lengthModSize = lengthMod();
        size_t bytesToWrite =
            std::min(len, static_cast<size_t>(kBlockContentSize - lengthModSize));
        if (len_ != 0 && lengthModSize == 0) {  // is full filled.
            allocateBlock();
            bytesToWrite = std::min(len, static_cast<size_t>(kBlockContentSize));
        }
        memcpy(tail_ + lengthModSize, value, bytesToWrite);
        len_ += bytesToWrite;

        if (bytesToWrite < len) {
            return write(value + bytesToWrite, len - bytesToWrite);
        } else {
            return *this;
        }
    }

    // stream
    ICord<kBlockContentSize>& operator<<(int8_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int8_t));
    }

    ICord<kBlockContentSize>& operator<<(uint8_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint8_t));
    }

    ICord<kBlockContentSize>& operator<<(int16_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int16_t));
    }

    ICord<kBlockContentSize>& operator<<(uint16_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint16_t));
    }

    ICord<kBlockContentSize>& operator<<(int32_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int32_t));
    }

    ICord<kBlockContentSize>& operator<<(uint32_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint32_t));
    }

    ICord<kBlockContentSize>& operator<<(int64_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int64_t));
    }

    ICord<kBlockContentSize>& operator<<(uint64_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint64_t));
    }

    ICord<kBlockContentSize>& operator<<(char value) {
        return write(&value, sizeof(char));
    }

    ICord<kBlockContentSize>& operator<<(bool value) {
        return write(reinterpret_cast<char*>(&value), sizeof(bool));
    }

    ICord<kBlockContentSize>& operator<<(float value) {
        return write(reinterpret_cast<char*>(&value), sizeof(float));
    }

    ICord<kBlockContentSize>& operator<<(double value) {
        return write(reinterpret_cast<char*>(&value), sizeof(double));
    }

    ICord<kBlockContentSize>& operator<<(const std::string& value) {
        return write(value.data(), value.size());
    }

    ICord<kBlockContentSize>& operator<<(const char* value) {
        return write(value, strlen(value));
    }

    ICord<kBlockContentSize>& operator<<(const ICord& rhs) {
        char* n = rhs.head_;
        while (n != rhs.tail_) {
            write(n, kBlockContentSize);
            // Get the pointer to the next block
            n = next(n);
        }

        // Last block
        write(rhs.tail_, rhs.lengthMod());

        return *this;
    }

private:
    // Disable dynamic allocation
    void* operator new(std::size_t) = delete;

    // Is the capacity full filled
    bool isFull() const {
        return len_ != 0 && lengthMod() == 0;
    }

    // Used size in last block
    std::size_t lengthMod() const {
        return len_ & (kBlockContentSize - 1);
    }

    // Is there only inline allocation
    bool isInline() const {
        return len_ < kBlockContentSize;
    }

    // return next block pointer
    char* next(char* p) const {
        if (p == tail_) {
            return nullptr;
        } else {
            return *reinterpret_cast<char**>(p + kBlockContentSize);
        }
    }

    void allocateBlock() {
        DCHECK(isFull());
        char* blk = reinterpret_cast<char*>(malloc(kBlockSize * sizeof(char)));

        if (tail_) {
            // Link the tail to the new block
            memcpy(tail_ + kBlockContentSize, reinterpret_cast<char*>(&blk), sizeof(char*));
        }
        tail_ = blk;

        if (!head_) {
            head_ = blk;
        }
    }

    static constexpr std::size_t kBlockSize = kBlockContentSize + sizeof(char*);

    size_t len_ = 0;
    char* head_;
    char* tail_;
    char inlineBlock_[kBlockSize];
};

}   // namespace nebula
