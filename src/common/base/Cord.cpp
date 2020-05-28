/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Cord.h"
#include "common/base/Logging.h"

namespace nebula {

Cord::Cord(int32_t blockSize)
        : blockSize_(blockSize)
        , blockContentSize_(blockSize_ - sizeof(char*))
        , blockPt_(blockContentSize_) {
}


Cord::~Cord() {
    clear();
}


void Cord::allocateBlock() {
    DCHECK_EQ(blockPt_, blockContentSize_);
    char* blk = reinterpret_cast<char*>(malloc(blockSize_ * sizeof(char)));
    CHECK(blk) << "Out of memory";

    if (tail_) {
        // Link the tail to the new block
        memcpy(tail_ + blockPt_, reinterpret_cast<char*>(&blk), sizeof(char*));
    }
    tail_ = blk;
    blockPt_ = 0;

    if (!head_) {
        head_ = blk;
    }
}


size_t Cord::size() const noexcept {
    return len_;
}


bool Cord::empty() const noexcept {
    return len_ == 0;
}


void Cord::clear() {
    if (head_) {
        DCHECK(tail_);

        // Need to release all blocks
        char* p = head_;
        while (p != tail_) {
            char* next;
            memcpy(reinterpret_cast<char*>(&next),
                   p + blockContentSize_,
                   sizeof(char*));
            free(p);
            p = next;
        }
        // Free the last block
        free(p);
    }

    blockPt_ = blockContentSize_;
    len_ = 0;

    head_ = nullptr;
    tail_ = nullptr;
}


bool Cord::applyTo(std::function<bool(const char*, int32_t)> visitor) const {
    if (empty()) {
        return true;
    }

    char* next = head_;
    while (next != tail_) {
        if (!visitor(next, blockContentSize_)) {
            // stop visiting further
            return false;
        }
        // Get the pointer to the next block
        memcpy(reinterpret_cast<char*>(&next),
               next + blockContentSize_,
               sizeof(char*));
    }

    // Last block
    return visitor(tail_, blockPt_);
}


size_t Cord::appendTo(std::string& str) const {
    if (empty()) {
        return 0;
    }

    char* next = head_;
    while (next != tail_) {
        str.append(next, blockContentSize_);
        // Get the pointer to the next block
        memcpy(reinterpret_cast<char*>(&next),
               next + blockContentSize_,
               sizeof(char*));
    }

    // Last block
    str.append(tail_, blockPt_);

    return len_;
}


std::string Cord::str() const {
    std::string buf;
    buf.reserve(len_);
    appendTo(buf);

    return buf;
}


Cord& Cord::write(const char* value, size_t len) {
    if (len == 0) {
        return *this;
    }

    size_t bytesToWrite =
        std::min(len, static_cast<size_t>(blockContentSize_ - blockPt_));
    if (bytesToWrite == 0) {
        allocateBlock();
        bytesToWrite =
            std::min(len, static_cast<size_t>(blockContentSize_));
    }
    memcpy(tail_ + blockPt_, value, bytesToWrite);
    blockPt_ += bytesToWrite;
    len_ += bytesToWrite;

    if (bytesToWrite < len) {
        return write(value + bytesToWrite, len - bytesToWrite);
    } else {
        return *this;
    }
}


/**********************
 *
 * Stream operator
 *
 *********************/
Cord& Cord::operator<<(int8_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(int8_t));
}


Cord& Cord::operator<<(uint8_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(uint8_t));
}


Cord& Cord::operator<<(int16_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(int16_t));
}


Cord& Cord::operator<<(uint16_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(uint16_t));
}


Cord& Cord::operator<<(int32_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(int32_t));
}


Cord& Cord::operator<<(uint32_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(uint32_t));
}


Cord& Cord::operator<<(int64_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(int64_t));
}


Cord& Cord::operator<<(uint64_t value) {
    return write(reinterpret_cast<char*>(&value), sizeof(uint64_t));
}


Cord& Cord::operator<<(char value) {
    return write(&value, sizeof(char));
}


Cord& Cord::operator<<(bool value) {
    return write(reinterpret_cast<char*>(&value), sizeof(bool));
}


Cord& Cord::operator<<(float value) {
    return write(reinterpret_cast<char*>(&value), sizeof(float));
}


Cord& Cord::operator<<(double value) {
    return write(reinterpret_cast<char*>(&value), sizeof(double));
}


Cord& Cord::operator<<(const std::string& value) {
    return write(value.data(), value.size());
}


Cord& Cord::operator<<(const char* value) {
    return write(value, strlen(value));
}

Cord& Cord::operator<<(const Cord& rhs) {
    char* next = rhs.head_;
    while (next != rhs.tail_) {
        write(next, blockContentSize_);
        // Get the pointer to the next block
        memcpy(reinterpret_cast<char*>(&next),
               next + blockContentSize_,
               sizeof(char*));
    }

    // Last block
    write(rhs.tail_, rhs.blockPt_);

    return *this;
}

}  // namespace nebula
