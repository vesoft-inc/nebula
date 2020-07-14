/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <base/Base.h>

namespace nebula {

template <std::size_t kBlockSize = 1024>
class ICord {
public:
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
        return visitor(tail_, len_ % kBlockContentSize);
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
        str.append(tail_,
                   len_ % kBlockContentSize == 0 ? kBlockContentSize : len_ % kBlockContentSize);

        return len_;
    }

    // Convert the cord content to a new string
    std::string str() const {
        std::string buf;
        buf.reserve(len_);
        appendTo(buf);

        return buf;
    }

    ICord<kBlockSize>& write(const char* value, size_t len) {
        if (len == 0) {
            return *this;
        }

        size_t bytesToWrite =
            std::min(len, static_cast<size_t>(kBlockContentSize - len_ % kBlockContentSize));
        if (isFull()) {
            allocateBlock();
            bytesToWrite = std::min(len, static_cast<size_t>(kBlockContentSize));
        }
        memcpy(tail_ + len_ % kBlockContentSize, value, bytesToWrite);
        len_ += bytesToWrite;

        if (bytesToWrite < len) {
            return write(value + bytesToWrite, len - bytesToWrite);
        } else {
            return *this;
        }
    }

    // stream
    ICord<kBlockSize>& operator<<(int8_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int8_t));
    }

    ICord<kBlockSize>& operator<<(uint8_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint8_t));
    }

    ICord<kBlockSize>& operator<<(int16_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int16_t));
    }

    ICord<kBlockSize>& operator<<(uint16_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint16_t));
    }

    ICord<kBlockSize>& operator<<(int32_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int32_t));
    }

    ICord<kBlockSize>& operator<<(uint32_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint32_t));
    }

    ICord<kBlockSize>& operator<<(int64_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(int64_t));
    }

    ICord<kBlockSize>& operator<<(uint64_t value) {
        return write(reinterpret_cast<char*>(&value), sizeof(uint64_t));
    }

    ICord<kBlockSize>& operator<<(char value) {
        return write(&value, sizeof(char));
    }

    ICord<kBlockSize>& operator<<(bool value) {
        return write(reinterpret_cast<char*>(&value), sizeof(bool));
    }

    ICord<kBlockSize>& operator<<(float value) {
        return write(reinterpret_cast<char*>(&value), sizeof(float));
    }

    ICord<kBlockSize>& operator<<(double value) {
        return write(reinterpret_cast<char*>(&value), sizeof(double));
    }

    ICord<kBlockSize>& operator<<(const std::string& value) {
        return write(value.data(), value.size());
    }

    ICord<kBlockSize>& operator<<(const char* value) {
        return write(value, strlen(value));
    }

    ICord<kBlockSize>& operator<<(const ICord& rhs) {
        char* n = rhs.head_;
        while (n != rhs.tail_) {
            write(n, kBlockContentSize);
            // Get the pointer to the next block
            n = next(n);
        }

        // Last block
        write(rhs.tail_, rhs.len_ % kBlockContentSize);

        return *this;
    }

private:
    // Disable dynamic allocation
    void* operator new(std::size_t) = delete;

    // Is the capacity full filled
    bool isFull() const {
        return len_ != 0 && len_ % kBlockContentSize == 0;
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

    static constexpr std::size_t kBlockContentSize = kBlockSize - sizeof(char*);

    size_t len_ = 0;
    char* head_;
    char* tail_;
    char inlineBlock_[kBlockSize];
};

}   // namespace nebula
