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

namespace nebula {
namespace graph {

class Iterator {
public:
    enum class Kind : uint8_t {
        kDefault,
        kGetNeighbors,
        kSequential,
        kUnion,
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

    virtual void erase() = 0;

    virtual const Row* row() const = 0;

    // Reset iterator position to `pos' from begin. Must be sure that the `pos' position
    // is lower than `size()' before resetting
    void reset(size_t pos = 0) {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        doReset(pos);
    }

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

    void erase() override {
        if (valid()) {
            iter_ = logicalRows_.erase(iter_);
        }
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
            auto vid = getColumn("_vid");
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

    void clear() {
        valid_ = false;
        dsIndices_.clear();
        logicalRows_.clear();
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
        if (valid()) {
            ++iter_;
        }
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
            DCHECK_LT(index->second, row->values.size());
            return row->values[index->second];
        }
    }

    const Row* row() const override {
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

    std::vector<const Row*>                      rows_;
    std::vector<const Row*>::iterator            iter_;
    std::unordered_map<std::string, int64_t>     colIndex_;
};

class UnionIterator final : public Iterator {
public:
    UnionIterator(std::unique_ptr<Iterator> left, std::unique_ptr<Iterator> right)
        : Iterator(left->valuePtr(), Kind::kUnion),
          left_(std::move(left)),
          right_(std::move(right)) {}

    std::unique_ptr<Iterator> copy() const override {
        auto iter = std::make_unique<UnionIterator>(left_->copy(), right_->copy());
        iter->reset();
        return iter;
    }

    bool valid() const override {
        return left_->valid() || right_->valid();
    }

    void next() override {
        if (left_->valid()) {
            left_->next();
        } else {
            if (right_->valid()) {
                right_->next();
            }
        }
    }

    size_t size() const override {
        return left_->size() + right_->size();
    }

    void erase() override {
        if (left_->valid()) {
            left_->erase();
        } else {
            if (right_->valid()) {
                right_->erase();
            }
        }
    }

    const Value& getColumn(const std::string& col) const override {
        if (left_->valid()) {
            return left_->getColumn(col);
        }
        if (right_->valid()) {
            return right_->getColumn(col);
        }
        return Value::kEmpty;
    }

    const Row* row() const override {
        if (left_->valid()) return left_->row();
        if (right_->valid()) return right_->row();
        return nullptr;
    }

private:
    void doReset(size_t poc) override {
        if (poc < left_->size()) {
            left_->reset(poc);
            right_->reset();
        } else {
            right_->reset(poc - left_->size());
        }
    }

    std::unique_ptr<Iterator> left_;
    std::unique_ptr<Iterator> right_;
};
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
