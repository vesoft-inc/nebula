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
    enum class Kind : uint8_t {
        kDefault,
        kGetNeighbors,
        kSequential,
    };

    explicit Iterator(std::shared_ptr<Value> value, Kind kind)
        : value_(value), kind_(kind) {}

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

    bool isGetNeighborsIter() const {
        return kind_ == Kind::kGetNeighbors;
    }

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

private:
    void doReset(size_t pos) override {
        iter_ = logicalRows_.begin() + pos;
    }

    void clear() {
        valid_ = false;
        colIndices_.clear();
        tagEdgeNameIndices_.clear();
        tagPropIndices_.clear();
        edgePropIndices_.clear();
        tagPropMaps_.clear();
        edgePropMaps_.clear();
        segments_.clear();
        logicalRows_.clear();
    }

    // Maps the origin column names with its column index, each response
    // has a segment.
    // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
    // -> {_vid : 0, _stats : 1, _tag:t1:p1:p2 : 2, _edge:d1:p1:p2 : 3}
    using ColumnIndex = std::vector<std::unordered_map<std::string, size_t>>;
    // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
    // -> {t1 : 2, e1 : 3}
    using TagEdgeNameIdxMap = std::unordered_map<size_t, std::string>;
    using TagEdgeNameIndex = std::vector<TagEdgeNameIdxMap>;

    // _tag:t1:p1:p2  ->  {t1 : {p1 : 0, p2 : 1}}
    // _edge:e1:p1:p2  ->  {e1 : {p1 : 0, p2 : 1}}
    using PropIdxMap = std::unordered_map<std::string, size_t>;
    // {tag/edge name : [column_idx, PropIdxMap]}
    using TagEdgePropIdxMap = std::unordered_map<std::string, std::pair<size_t, PropIdxMap>>;
    // Maps the property name with its index, each response has a segment
    // in PropIndex.
    using PropIndex = std::vector<TagEdgePropIdxMap>;

    // LogicalRow: <segment_id, row, edge_name, edge_props>
    using LogicalRow = std::tuple<size_t, const Row*, std::string, const List*>;

    using PropList = std::vector<std::string>;
    // _tag:t1:p1:p2  ->  {t1 : [column_idx, {p1, p2}]}
    // _edge:e1:p1:p2  ->  {e1 : [columns_idx, {p1, p2}]}
    using TagEdgePropMap = std::unordered_map<std::string, std::pair<size_t, PropList>>;
    // Maps the tag/edge with its properties, each response has a segment
    // in PropMaps
    using PropMaps = std::vector<TagEdgePropMap>;

    inline size_t currentSeg() const {
        auto& current = *iter_;
        return std::get<0>(current);
    }

    inline const Row* currentRow() const {
        auto& current = *iter_;
        return std::get<1>(current);
    }

    inline const std::string& currentEdgeName() const {
        auto& current = *iter_;
        return std::get<2>(current);
    }

    inline const List* currentEdgeProps() const {
        auto& current = *iter_;
        return std::get<3>(current);
    }

    StatusOr<int64_t> buildIndex(const std::vector<std::string>& colNames);

    Status buildPropIndex(const std::string& props,
                          size_t columnId,
                          bool isEdge,
                          TagEdgeNameIdxMap& tagEdgeNameIndex,
                          TagEdgePropIdxMap& tagEdgePropIdxMap,
                          TagEdgePropMap& tagEdgePropMap);

    friend class IteratorTest_TestHead_Test;
    bool                                    valid_{false};
    ColumnIndex                             colIndices_;
    TagEdgeNameIndex                        tagEdgeNameIndices_;
    PropIndex                               tagPropIndices_;
    PropIndex                               edgePropIndices_;
    PropMaps                                tagPropMaps_;
    PropMaps                                edgePropMaps_;
    std::vector<const DataSet*>             segments_;
    std::vector<LogicalRow>                 logicalRows_;
    std::vector<LogicalRow>::iterator       iter_;
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
            DCHECK_LT(index->second, row->values.size());
            return row->values[index->second];
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
