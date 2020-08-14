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

class LogicalRow {
public:
    enum class Kind : uint8_t {
        kGetNeighbors,
        kSequential,
        kJoin,
    };

    virtual ~LogicalRow() {}
    virtual const Value& operator[](size_t) const = 0;
    virtual size_t size() const = 0;
    virtual Kind kind() const = 0;
    virtual std::vector<const Row*> segments() const = 0;
};

class Iterator {
public:
    template <typename T>
    using RowsType = std::vector<T>;
    template <typename T>
    using RowsIter = typename RowsType<T>::iterator;

    enum class Kind : uint8_t {
        kDefault,
        kGetNeighbors,
        kSequential,
        kJoin,
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

    virtual const LogicalRow* row() const = 0;

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

    virtual size_t size() const = 0;

    bool empty() const {
        return size() == 0;
    }

    bool isDefaultIter() const {
        return kind_ == Kind::kDefault;
    }

    bool isGetNeighborsIter() const {
        return kind_ == Kind::kGetNeighbors;
    }

    bool isSequentialIter() const {
        return kind_ == Kind::kSequential;
    }

    bool isJoinIter() const {
        return kind_ == Kind::kJoin;
    }

    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getColumn(const std::string& col) const = 0;

    virtual const Value& getTagProp(const std::string&,
                                    const std::string&) const {
        DLOG(FATAL) << "Shouldn't call the unimplemented method";
        return Value::kEmpty;
    }

    virtual const Value& getEdgeProp(const std::string&,
                                     const std::string&) const {
        DLOG(FATAL) << "Shouldn't call the unimplemented method";
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
        DLOG(FATAL) << "This method should not be invoked";
        return Value::kEmpty;
    }

    const LogicalRow* row() const override {
        DLOG(FATAL) << "This method should not be invoked";
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
    // Its unique based on the plan
    List getVertices() {
        DCHECK(iter_ == logicalRows_.begin());
        List vertices;
        for (; valid(); next()) {
            vertices.values.emplace_back(getVertex());
        }
        reset();
        return vertices;
    }

    // Its unique based on the GN interface dedup
    List getEdges() {
        DCHECK(iter_ == logicalRows_.begin());
        List edges;
        for (; valid(); next()) {
            edges.values.emplace_back(getEdge());
        }
        reset();
        return edges;
    }

    const LogicalRow* row() const override {
        return &*iter_;
    }

private:
    void doReset(size_t pos) override {
        iter_ = logicalRows_.begin() + pos;
    }

    inline size_t currentSeg() const {
        return iter_->dsIdx_;
    }

    inline const std::string& currentEdgeName() const {
        return iter_->edgeName_;
    }

    inline const List* currentEdgeProps() const {
        return iter_->edgeProps_;
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

    class GetNbrLogicalRow final : public LogicalRow {
    public:
        GetNbrLogicalRow(size_t dsIdx, const Row* row, std::string edgeName, const List* edgeProps)
            : dsIdx_(dsIdx), row_(row), edgeName_(std::move(edgeName)), edgeProps_(edgeProps) {}

        const Value& operator[](size_t idx) const override {
            if (idx < row_->size()) {
                return (*row_)[idx];
            } else {
                return Value::kNullOverflow;
            }
        }

        size_t size() const override {
            return row_->size();
        }

        LogicalRow::Kind kind() const override {
            return Kind::kGetNeighbors;
        }

        std::vector<const Row*> segments() const override {
            return { row_ };
        }

    private:
        friend class GetNeighborsIter;
        size_t dsIdx_;
        const Row* row_;
        std::string edgeName_;
        const List* edgeProps_;
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

    bool                       valid_{false};
    RowsType<GetNbrLogicalRow> logicalRows_;
    RowsIter<GetNbrLogicalRow> iter_;
    std::vector<DataSetIndex>  dsIndices_;
};

class SequentialIter final : public Iterator {
public:
    class SeqLogicalRow final : public LogicalRow {
    public:
        explicit SeqLogicalRow(const Row* row) : row_(row) {}

        const Value& operator[](size_t idx) const override {
            if (idx < row_->size()) {
                return row_->values[idx];
            } else {
                return Value::kEmpty;
            }
        }

        size_t size() const override {
            return row_->size();
        }

        LogicalRow::Kind kind() const override {
            return Kind::kSequential;
        }

        std::vector<const Row*> segments() const override {
            return { row_ };
        }

    private:
        friend class SequentialIter;
        const Row* row_;
    };

    explicit SequentialIter(std::shared_ptr<Value> value)
        : Iterator(value, Kind::kSequential) {
        DCHECK(value->isDataSet());
        auto& ds = value->getDataSet();
        for (auto& row : ds.rows) {
            rows_.emplace_back(&row);
        }
        iter_ = rows_.begin();
        for (size_t i = 0; i < ds.colNames.size(); ++i) {
            colIndices_.emplace(ds.colNames[i], i);
        }
    }

    // union two sequential iterator.
    SequentialIter(std::unique_ptr<Iterator> left, std::unique_ptr<Iterator> right)
        : Iterator(left->valuePtr(), Kind::kSequential) {
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
        colIndices_ = lIter->getColIndices();
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

    RowsIter<SeqLogicalRow> begin() {
        return rows_.begin();
    }

    RowsIter<SeqLogicalRow> end() {
        return rows_.end();
    }

    const std::unordered_map<std::string, int64_t>& getColIndices() const {
        return colIndices_;
    }

    size_t size() const override {
        return rows_.size();
    }

    const Value& getColumn(const std::string& col) const override {
        if (!valid()) {
            return Value::kNullValue;
        }
        auto logicalRow = *iter_;
        auto index = colIndices_.find(col);
        if (index == colIndices_.end()) {
            return Value::kNullValue;
        } else {
            DCHECK_LT(index->second, logicalRow.row_->values.size());
            return logicalRow.row_->values[index->second];
        }
    }

    // TODO: We should build new iter for get props, the seq iter will
    // not meet the requirements of match any more.
    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override {
        return getColumn(tag+ "." + prop);
    }

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override {
        return getColumn(edge + "." + prop);
    }

protected:
    const LogicalRow* row() const override {
        if (!valid()) {
            return nullptr;
        }
        return &*iter_;
    }

protected:
    // Notice: We only use this interface when return results to client.
    friend class DataCollectExecutor;
    Row&& moveRow() {
        return std::move(*const_cast<Row*>(iter_->row_));
    }

private:
    void doReset(size_t pos) override {
        iter_ = rows_.begin() + pos;
    }

private:
    RowsType<SeqLogicalRow>                      rows_;
    RowsIter<SeqLogicalRow>                      iter_;
    std::unordered_map<std::string, int64_t>     colIndices_;
};

class JoinIter final : public Iterator {
public:
    class JoinLogicalRow final : public LogicalRow {
    public:
        explicit JoinLogicalRow(
            std::vector<const Row*> values,
            size_t size,
            const std::unordered_map<size_t, std::pair<size_t, size_t>>* colIdxIndices)
            : values_(std::move(values)), size_(size), colIdxIndices_(colIdxIndices) {}

        const Value& operator[](size_t idx) const override {
            if (idx < size_) {
                auto index = colIdxIndices_->find(idx);
                if (index == colIdxIndices_->end()) {
                    return Value::kNullValue;
                } else {
                    auto keyIdx = index->second.first;
                    auto valIdx = index->second.second;
                    DCHECK_LT(keyIdx, values_.size());
                    DCHECK_LT(valIdx, values_[keyIdx]->values.size());
                    return values_[keyIdx]->values[valIdx];
                }
            } else {
                return Value::kEmpty;
            }
        }

        size_t size() const override {
            return size_;
        }

        LogicalRow::Kind kind() const override {
            return Kind::kJoin;
        }

        std::vector<const Row*> segments() const override {
            return values_;
        }

    private:
        friend class JoinIter;
        std::vector<const Row*> values_;
        size_t size_;
        const std::unordered_map<size_t, std::pair<size_t, size_t>>* colIdxIndices_;
    };

    JoinIter() : Iterator(nullptr, Kind::kJoin) {}

    void joinIndex(const Iterator* lhs, const Iterator* rhs);

    void addRow(JoinLogicalRow row) {
        rows_.emplace_back(std::move(row));
        iter_ = rows_.begin();
    }

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<JoinIter>(*this);
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

    RowsIter<JoinLogicalRow> begin() {
        return rows_.begin();
    }

    RowsIter<JoinLogicalRow> end() {
        return rows_.end();
    }

    const std::unordered_map<std::string, std::pair<size_t, size_t>>&
    getColIndices() const {
        return colIndices_;
    }

    const std::unordered_map<size_t, std::pair<size_t, size_t>>&
    getColIdxIndices() const {
        return colIdxIndices_;
    }

    size_t size() const override {
        return rows_.size();
    }

    const Value& getColumn(const std::string& col) const override {
        if (!valid()) {
            return Value::kNullValue;
        }
        auto row = *iter_;
        auto index = colIndices_.find(col);
        if (index == colIndices_.end()) {
            return Value::kNullValue;
        } else {
            auto segIdx = index->second.first;
            auto colIdx = index->second.second;
            DCHECK_LT(segIdx, row.values_.size());
            DCHECK_LT(colIdx, row.values_[segIdx]->values.size());
            return row.values_[segIdx]->values[colIdx];
        }
    }

    const LogicalRow* row() const override {
        if (!valid()) {
            return nullptr;
        }
        return &*iter_;
    }

private:
    void doReset(size_t pos) override {
        iter_ = rows_.begin() + pos;
    }

    size_t buildIndexFromSeqIter(const SequentialIter* iter, size_t segIdx);

    size_t buildIndexFromJoinIter(const JoinIter* iter, size_t segIdx);

private:
    RowsType<JoinLogicalRow>                                       rows_;
    RowsIter<JoinLogicalRow>                                       iter_;
    // colName -> segIdx, currentSegColIdx
    std::unordered_map<std::string, std::pair<size_t, size_t>>     colIndices_;
    // colIdx -> segIdx, currentSegColIdx
    std::unordered_map<size_t, std::pair<size_t, size_t>>          colIdxIndices_;
};

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind);
std::ostream& operator<<(std::ostream& os, LogicalRow::Kind kind);
std::ostream& operator<<(std::ostream& os, const LogicalRow& row);

}  // namespace graph
}  // namespace nebula

namespace std {

template <>
struct equal_to<const nebula::Row*> {
    bool operator()(const nebula::Row* lhs, const nebula::Row* rhs) const {
        return lhs == rhs
                   ? true
                   : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
    }
};

template <>
struct equal_to<const nebula::graph::LogicalRow*> {
    bool operator()(const nebula::graph::LogicalRow* lhs,
                    const nebula::graph::LogicalRow* rhs) const;
};

template <>
struct hash<const nebula::Row*> {
    size_t operator()(const nebula::Row* row) const {
        return !row ? 0 : hash<nebula::Row>()(*row);
    }
};

template <>
struct hash<nebula::graph::LogicalRow> {
    size_t operator()(const nebula::graph::LogicalRow& row) const {
        size_t seed = 0;
        for (auto& value : row.segments()) {
            seed ^= std::hash<const nebula::Row*>()(value);
        }
        return seed;
    }
};

template <>
struct hash<const nebula::graph::LogicalRow*> {
    size_t operator()(const nebula::graph::LogicalRow* row) const {
        return !row ? 0 : hash<nebula::graph::LogicalRow>()(*row);
    }
};

}  // namespace std

#endif  // CONTEXT_ITERATOR_H_
