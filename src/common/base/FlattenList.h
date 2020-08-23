/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_GRAPH_FLATTENLIST_H
#define NEBULA_GRAPH_FLATTENLIST_H

#include "Base.h"
#include "ICord.h"
#include "Status.h"
#include "StatusOr.h"

namespace nebula {

class FlattenListWriter {
public:
    FlattenListWriter& operator<<(int64_t v);
    FlattenListWriter& operator<<(bool v);
    FlattenListWriter& operator<<(double v);
    FlattenListWriter& operator<<(const char* v);
    FlattenListWriter& operator<<(const std::string &v);
    FlattenListWriter& operator<<(folly::StringPiece v);
    FlattenListWriter& operator<<(const VariantType &v);
    std::string finish();

private:
    ICord<> cord_;
};

class FlattenListReader {

public:
    class Iterator final {
    public:
        const StatusOr<VariantType>& operator*() const;
        const StatusOr<VariantType>* operator->() const;
        Iterator& operator++();
        bool operator==(const Iterator& rhs) const noexcept {
            return s_.data() == rhs.s_.data() && offset_ == rhs.offset_;
        }
        bool operator!=(const Iterator& rhs) const noexcept {
            return !this->operator==(rhs);
        }
        StatusOr<folly::StringPiece> raw() const;
    private:
        friend class FlattenListReader;
        explicit Iterator(const FlattenListReader& r, bool end = false);
        size_t remain() const;
        void load() const;

    private:
        mutable StatusOr<VariantType> v_;
        mutable size_t size_ = 0;
        folly::StringPiece s_;
        size_t offset_ = 0;
    };
public:
    explicit FlattenListReader(folly::StringPiece s) : s_(s) {};

    Iterator begin() {
        return Iterator(*this, false);
    }

    Iterator end() {
        return Iterator(*this, true);
    }
private:
    folly::StringPiece s_;
};

} // namespace nebula
#endif   // NEBULA_GRAPH_FLATTENLIST_H
