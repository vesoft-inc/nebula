/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_ITERATOR_H_
#define CONTEXT_ITERATOR_H_

#include <memory>

#include "common/datatypes/Value.h"
#include "common/datatypes/List.h"
#include "common/datatypes/DataSet.h"

namespace nebula {
namespace graph {

class Iterator {
public:
    explicit Iterator(std::shared_ptr<Value> value) : value_(value) {}

    virtual ~Iterator() = default;

    virtual std::unique_ptr<Iterator> copy() const = 0;

    virtual bool valid() const = 0;

    virtual void next() = 0;

    virtual void erase() = 0;

    // Reset iterator position to `pos' from begin. Must be sure that the `pos' position
    // is lower than `size()' before resetting
    void reset(size_t pos = 0) {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        doReset(pos);
    }

    void operator++() {
        next();
    }

    virtual const Value& value() const {
        return *value_;
    }

    const Value& operator*() const {
        return value();
    }

    virtual size_t size() const = 0;

    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getColumn(const std::string& col) const {
        UNUSED(col);
        return Value::kEmpty;
    }

    virtual const Value& getTagProp(const std::string& tag,
                                    const std::string& prop) const {
        UNUSED(tag);
        UNUSED(prop);
        return Value::kEmpty;
    }

    virtual const Value& getEdgeProp(const std::string& edge,
                                     const std::string& prop) const {
        UNUSED(edge);
        UNUSED(prop);
        return Value::kEmpty;
    }

protected:
    virtual void doReset(size_t pos) = 0;

    std::shared_ptr<Value> value_;
};

class DefaultIter final : public Iterator {
public:
    explicit DefaultIter(std::shared_ptr<Value> value) : Iterator(value) {}

    std::unique_ptr<Iterator> copy() const override {
        return std::make_unique<DefaultIter>(*this);
    }

    bool valid() const override {
        return !(counter_ > 0);
    }

    void next() override {
        counter_++;
    }

    void erase() override {
        counter_--;
    }

    size_t size() const override {
        return 1;
    }

private:
    void doReset(size_t pos) override {
        counter_ = pos;
    }

    int64_t counter_{0};
};

class GetNeighborsIter final : public Iterator {
public:
    explicit GetNeighborsIter(std::shared_ptr<Value> value);

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<GetNeighborsIter>(*this);
        copy->reset();
        return copy;
    }

    bool valid() const override {
        return iter_ < edges_.end();
    }

    void next() override {
        if (!valid()) {
            return;
        }
        ++iter_;
    }

    void erase() override {
        iter_ = edges_.erase(iter_);
    }

    size_t size() const override {
        return edges_.size();
    }

    const Value& getColumn(const std::string& col) const override;

    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override;

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override;

private:
    void doReset(size_t pos) override {
        iter_ = edges_.begin() + pos;
    }

    int64_t buildIndex(const std::vector<std::string>& colNames);

    std::pair<std::string, std::unordered_map<std::string, int64_t>>
    buildPropIndex(const std::string& props);

private:
    std::vector<std::unordered_map<std::string, int64_t>> colIndex_;
    using PropIdxMap = std::unordered_map<std::string, int64_t>;
    using TagEdgePropMap = std::unordered_map<std::string, PropIdxMap>;
    using PropIndex = std::vector<TagEdgePropMap>;
    // Edge: <segment_id, row, column_id, edge_props>
    using Edge = std::tuple<int64_t, const Row*, int64_t, const List*>;
    PropIndex                      tagPropIndex_;
    PropIndex                      edgePropIndex_;
    std::vector<const DataSet*>    segments_;
    std::vector<Edge>              edges_;
    std::vector<Edge>::iterator    iter_;
};

class SequentialIter final : public Iterator {
public:
    explicit SequentialIter(std::shared_ptr<Value> value) : Iterator(value) {
        DCHECK(value->isDataSet());
        auto& ds = value->getDataSet();
        for (auto& row : ds.rows) {
            rows_.emplace_back(&row);
        }
        iter_ = rows_.begin();
        for (size_t i = 0; i < ds.colNames.size(); ++i) {
            colIndex_.emplace(ds.colNames[i], i);
        }
    }

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<SequentialIter>(*this);
        copy->reset();
        return copy;
    }

    bool valid() const override {
        return iter_ < rows_.end();
    }

    void next() override {
        if (!valid()) {
            return;
        }
        ++iter_;
    }

    void erase() override {
        iter_ = rows_.erase(iter_);
    }

    size_t size() const override {
        return rows_.size();
    }

    const Value& getColumn(const std::string& col) const override {
        if (!valid()) {
            return Value::kNullValue;
        }
        auto row = *iter_;
        auto index = colIndex_.find(col);
        if (index == colIndex_.end()) {
            return Value::kNullValue;
        } else {
            DCHECK_LT(index->second, row->columns.size());
            return row->columns[index->second];
        }
    }

private:
    void doReset(size_t pos) override {
        iter_ = rows_.begin() + pos;
    }

    std::vector<const Row*>                      rows_;
    std::vector<const Row*>::iterator            iter_;
    std::unordered_map<std::string, int64_t>     colIndex_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_ITERATOR_H_
