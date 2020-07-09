/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_ITERATOR_H_
#define CONTEXT_ITERATOR_H_

#include <memory>

#include <gtest/gtest_prod.h>

#include "common/datatypes/Value.h"
#include "common/datatypes/List.h"
#include "common/datatypes/DataSet.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class Iterator {
public:
    enum class Kind : uint8_t {
        kDefault,
        kGetNeighbors,
        kSequential,
    };

    explicit Iterator(std::shared_ptr<Value> value, Kind kind)
        : value_(value), kind_(kind) {}

    virtual ~Iterator() = default;

    Kind kind() const {
        return kind_;
    }

    virtual std::unique_ptr<Iterator> copy() const = 0;

    virtual bool valid() const = 0;

    virtual void next() = 0;

    // erase current iter
    virtual void erase() = 0;

    virtual const Row* row() const = 0;

    // erase range, no include last position, if last > size(), erase to the end position
    virtual void eraseRange(size_t first, size_t last) = 0;

    // Reset iterator position to `pos' from begin. Must be sure that the `pos' position
    // is lower than `size()' before resetting
    void reset(size_t pos = 0) {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        doReset(pos);
    }

    virtual void clear() = 0;

    void operator++() {
        next();
    }

    virtual std::shared_ptr<Value> valuePtr() const {
        return value_;
    }

    virtual const Value& value() const {
        return *value_;
    }

    const Value& operator*() const {
        return value();
    }

    virtual size_t size() const = 0;

    bool isDefaultIter() const {
        return kind_ == Kind::kDefault;
    }

    bool isGetNeighborsIter() const {
        return kind_ == Kind::kGetNeighbors;
    }

    bool isSequentialIter() const {
        return kind_ == Kind::kSequential;
    }

    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getColumn(const std::string& col) const = 0;

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

    virtual Value getVertex() const {
        return Value();
    }

    virtual Value getEdge() const {
        return Value();
    }

protected:
    virtual void doReset(size_t pos) = 0;

    std::shared_ptr<Value> value_;
    Kind                   kind_;
};

class DefaultIter final : public Iterator {
public:
    explicit DefaultIter(std::shared_ptr<Value> value)
        : Iterator(value, Kind::kDefault) {}

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

    void eraseRange(size_t, size_t) override {
        return;
    }

    void clear() override {
        reset();
    }

    size_t size() const override {
        return 1;
    }

    const Value& getColumn(const std::string& /* col */) const override {
        return Value::kEmpty;
    }

    const Row* row() const override {
        return nullptr;
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
        return valid_ && iter_ < logicalRows_.end();
    }

    void next() override {
        if (valid()) {
            ++iter_;
        }
    }

    void clear() override {
        valid_ = false;
        dsIndices_.clear();
        logicalRows_.clear();
    }

    void erase() override {
        if (valid()) {
            iter_ = logicalRows_.erase(iter_);
        }
    }

    void eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return;
        }
        if (last > size()) {
            logicalRows_.erase(logicalRows_.begin() + first, logicalRows_.end());
        } else {
            logicalRows_.erase(logicalRows_.begin() + first, logicalRows_.begin() + last);
        }
        reset();
    }

    size_t size() const override {
        return logicalRows_.size();
    }

    const Value& getColumn(const std::string& col) const override;

    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override;

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override;

    Value getVertex() const override;

    Value getEdge() const override;

    // getVertices and getEdges arg batch interface use for subgraph
    List getVertices() {
        DCHECK(iter_ == logicalRows_.begin());
        List vertices;
        std::unordered_set<Value> vids;
        for (; valid(); next()) {
            auto vid = getColumn(kVid);
            if (vid.isNull()) {
                continue;
            }
            auto found = vids.find(vid);
            if (found == vids.end()) {
                vertices.values.emplace_back(getVertex());
                vids.emplace(std::move(vid));
            }
        }
        reset();
        return vertices;
    }

    List getEdges() {
        DCHECK(iter_ == logicalRows_.begin());
        List edges;
        for (; valid(); next()) {
            edges.values.emplace_back(getEdge());
        }
        reset();
        return edges;
    }

    const Row* row() const override {
        return iter_->row;
    }

private:
    void doReset(size_t pos) override {
        iter_ = logicalRows_.begin() + pos;
    }

    inline size_t currentSeg() const {
        return iter_->dsIdx;
    }

    inline const std::string& currentEdgeName() const {
        return iter_->edgeName;
    }

    inline const List* currentEdgeProps() const {
        return iter_->edgeProps;
    }

    struct PropIndex {
        size_t colIdx;
        std::vector<std::string> propList;
        std::unordered_map<std::string, size_t> propIndices;
    };

    struct DataSetIndex {
        const DataSet* ds;
        // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
        // -> {_vid : 0, _stats : 1, _tag:t1:p1:p2 : 2, _edge:d1:p1:p2 : 3}
        std::unordered_map<std::string, size_t> colIndices;
        // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
        // -> {2 : t1, 3 : e1}
        std::unordered_map<size_t, std::string> tagEdgeNameIndices;
        // _tag:t1:p1:p2  ->  {t1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
        std::unordered_map<std::string, PropIndex> tagPropsMap;
        // _edge:e1:p1:p2  ->  {e1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
        std::unordered_map<std::string, PropIndex> edgePropsMap;
    };

    struct LogicalRow {
        size_t dsIdx;
        const Row* row;
        std::string edgeName;
        const List* edgeProps;
    };

    StatusOr<int64_t> buildIndex(DataSetIndex* dsIndex);
    Status buildPropIndex(const std::string& props,
                          size_t columnId,
                          bool isEdge,
                          DataSetIndex* dsIndex);
    Status processList(std::shared_ptr<Value> value);
    StatusOr<DataSetIndex> makeDataSetIndex(const DataSet& ds, size_t idx);
    void makeLogicalRowByEdge(int64_t edgeStartIndex, size_t idx, const DataSetIndex& dsIndex);

    FRIEND_TEST(IteratorTest, TestHead);

    bool valid_{false};
    std::vector<LogicalRow> logicalRows_;
    std::vector<LogicalRow>::iterator iter_;
    std::vector<DataSetIndex> dsIndices_;
};

class SequentialIter final : public Iterator {
public:
    explicit SequentialIter(std::shared_ptr<Value> value)
        : Iterator(value, Kind::kSequential) {
        DCHECK(value->isDataSet());
        auto& ds = value->getDataSet();
        for (auto& row : ds.rows) {
            rows_.emplace_back(&row);
        }
        iter_ = rows_.begin();
        for (size_t i = 0; i < ds.colNames.size(); ++i) {
            colIndexes_.emplace(ds.colNames[i], i);
        }
    }

    SequentialIter(std::unique_ptr<Iterator> left, std::unique_ptr<Iterator> right)
        : Iterator(nullptr, Kind::kSequential) {
        DCHECK(left->isSequentialIter());
        DCHECK(right->isSequentialIter());
        auto lIter = static_cast<SequentialIter*>(left.get());
        auto rIter = static_cast<SequentialIter*>(right.get());
        rows_.insert(rows_.end(),
                     std::make_move_iterator(lIter->begin()),
                     std::make_move_iterator(lIter->end()));

        rows_.insert(rows_.end(),
                     std::make_move_iterator(rIter->begin()),
                     std::make_move_iterator(rIter->end()));
        iter_ = rows_.begin();
        colIndexes_ = lIter->getColIndexes();
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
        if (valid()) {
            ++iter_;
        }
    }

    void erase() override {
        iter_ = rows_.erase(iter_);
    }

    void eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return;
        }
        if (last > size()) {
            rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
        reset();
    }

    void clear() override {
        rows_.clear();
        reset();
    }

    std::vector<const Row*>::iterator begin() {
        return rows_.begin();
    }

    std::vector<const Row*>::iterator end() {
        return rows_.end();
    }

    const std::unordered_map<std::string, int64_t>& getColIndexes() const {
        return colIndexes_;
    }

    size_t size() const override {
        return rows_.size();
    }

    const Value& getColumn(const std::string& col) const override {
        if (!valid()) {
            return Value::kNullValue;
        }
        auto row = *iter_;
        auto index = colIndexes_.find(col);
        if (index == colIndexes_.end()) {
            return Value::kNullValue;
        } else {
            DCHECK_LT(index->second, row->values.size());
            return row->values[index->second];
        }
    }

    const Row* row() const override {
        if (!valid()) {
            return nullptr;
        }
        return *iter_;
    }

protected:
    // Notice: We only use this interface when return results to client.
    friend class DataCollectExecutor;
    Row&& moveRow() {
        return std::move(*const_cast<Row*>(*iter_));
    }

private:
    void doReset(size_t pos) override {
        iter_ = rows_.begin() + pos;
    }

private:
    std::vector<const Row*>                      rows_;
    std::vector<const Row*>::iterator            iter_;
    std::unordered_map<std::string, int64_t>     colIndexes_;
};

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind);

}  // namespace graph
}  // namespace nebula
namespace std {
template <>
struct equal_to<const nebula::Row*> {
    bool operator()(const nebula::Row* lhs, const nebula::Row* rhs) const {
        return *lhs == *rhs;
    }
};

template <>
struct hash<const nebula::Row*> {
    size_t operator()(const nebula::Row* row) const {
        return hash<nebula::Row>()(*row);
    }
};

}   // namespace std
#endif  // CONTEXT_ITERATOR_H_
