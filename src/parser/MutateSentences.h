/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_MUTATESENTENCES_H_
#define PARSER_MUTATESENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class PropertyList final {
public:
    void addProp(std::string *propname) {
        properties_.emplace_back(propname);
    }

    std::string toString() const;

    std::vector<std::string*> properties() const {
        std::vector<std::string*> result;
        result.resize(properties_.size());
        auto get = [] (const auto &ptr) { return ptr.get(); };
        std::transform(properties_.begin(), properties_.end(), result.begin(), get);
        return result;
    }

private:
    std::vector<std::unique_ptr<std::string>>   properties_;
};


class VertexTagItem final {
 public:
     explicit VertexTagItem(std::string *tagName, PropertyList *properties = nullptr) {
         tagName_.reset(tagName);
         properties_.reset(properties);
     }

     std::string toString() const;

     const std::string* tagName() const {
         return tagName_.get();
     }

     std::vector<std::string*> properties() const {
         return properties_->properties();
     }

 private:
     std::unique_ptr<std::string>               tagName_;
     std::unique_ptr<PropertyList>              properties_;
};


class VertexTagList final {
 public:
     void addTagItem(VertexTagItem *tagItem) {
         tagItems_.emplace_back(tagItem);
     }

     std::string toString() const;

     std::vector<VertexTagItem*> tagItems() const {
         std::vector<VertexTagItem*> result;
         result.reserve(tagItems_.size());
         for (auto &item : tagItems_) {
             result.emplace_back(item.get());
         }
         return result;
     }

 private:
     std::vector<std::unique_ptr<VertexTagItem>>    tagItems_;
};


class ValueList final {
public:
    void addValue(Expression *value) {
        values_.emplace_back(value);
    }

    std::string toString() const;

    std::vector<Expression*> values() const {
        std::vector<Expression*> result;
        result.resize(values_.size());
        auto get = [] (const auto &ptr) { return ptr.get(); };
        std::transform(values_.begin(), values_.end(), result.begin(), get);
        return result;
    }

private:
    std::vector<std::unique_ptr<Expression>>    values_;
};


class VertexRowItem final {
public:
    VertexRowItem(int64_t id, ValueList *values) {
        id_ = id;
        values_.reset(values);
    }

    int64_t id() const {
        return id_;
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const;

private:
    int64_t                                     id_;
    std::unique_ptr<ValueList>                  values_;
};


class VertexRowList final {
public:
    void addRow(VertexRowItem *row) {
        rows_.emplace_back(row);
    }

    /**
     * For now, we haven't execution plan cache supported.
     * So to avoid too many deep copying, we return the fields or nodes
     * of kinds of parsing tree in such a shallow copy way,
     * just like in all other places.
     * In the future, we might do deep copy to the plan,
     * of course excluding the volatile arguments in queries.
     */
    std::vector<VertexRowItem*> rows() const {
        std::vector<VertexRowItem*> result;
        result.resize(rows_.size());
        auto get = [] (const auto &ptr) { return ptr.get(); };
        std::transform(rows_.begin(), rows_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<VertexRowItem>> rows_;
};


class InsertVertexSentence final : public Sentence {
public:
    InsertVertexSentence(VertexTagList *tagList,
                         VertexRowList *rows,
                         bool overwritable = true) {
        tagList_.reset(tagList);
        rows_.reset(rows);
        overwritable_ = overwritable;
        kind_ = Kind::kInsertVertex;
    }

    bool overwritable() const {
        return overwritable_;
    }

    auto tagItems() const {
        return tagList_->tagItems();
    }

    std::vector<VertexRowItem*> rows() const {
        return rows_->rows();
    }

    std::string toString() const override;

private:
    bool                                        overwritable_{true};
    std::unique_ptr<VertexTagList>              tagList_;
    std::unique_ptr<VertexRowList>              rows_;
};


class EdgeRowItem final {
public:
    EdgeRowItem(int64_t srcid, int64_t dstid, ValueList *values) {
        srcid_ = srcid;
        dstid_ = dstid;
        values_.reset(values);
    }

    EdgeRowItem(int64_t srcid, int64_t dstid, int64_t rank, ValueList *values) {
        srcid_ = srcid;
        dstid_ = dstid;
        rank_ = rank;
        values_.reset(values);
    }

    int64_t srcid() const {
        return srcid_;
    }

    int64_t dstid() const {
        return dstid_;
    }

    int64_t rank() const {
        return rank_;
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const;

private:
    int64_t                                     srcid_{0};
    int64_t                                     dstid_{0};
    int64_t                                     rank_{0};
    std::unique_ptr<ValueList>                  values_;
};


class EdgeRowList final {
public:
    void addRow(EdgeRowItem *row) {
        rows_.emplace_back(row);
    }

    std::vector<EdgeRowItem*> rows() const {
        std::vector<EdgeRowItem*> result;
        result.resize(rows_.size());
        auto get = [] (const auto &ptr) { return ptr.get(); };
        std::transform(rows_.begin(), rows_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<EdgeRowItem>>   rows_;
};


class InsertEdgeSentence final : public Sentence {
public:
    InsertEdgeSentence() {
        kind_ = Kind::kInsertEdge;
    }
    void setOverwrite(bool overwritable) {
        overwritable_ = overwritable;
    }

    bool overwritable() const {
        return overwritable_;
    }

    void setEdge(std::string *edge) {
        edge_.reset(edge);
    }

    const std::string* edge() const {
        return edge_.get();
    }

    void setProps(PropertyList *props) {
        properties_.reset(props);
    }

    std::vector<std::string*> properties() const {
        return properties_->properties();
    }

    void setRows(EdgeRowList *rows) {
        rows_.reset(rows);
    }

    std::vector<EdgeRowItem*> rows() const {
        return rows_->rows();
    }

    std::string toString() const override;

private:
    bool                                        overwritable_{true};
    std::unique_ptr<std::string>                edge_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<EdgeRowList>                rows_;
};


class UpdateItem final {
public:
    UpdateItem(std::string *field, Expression *value) {
        field_.reset(field);
        value_.reset(value);
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>                field_;
    std::unique_ptr<Expression>                 value_;
};


class UpdateList final {
public:
    void addItem(UpdateItem *item) {
        items_.emplace_back(item);
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<UpdateItem>>    items_;
};


class UpdateVertexSentence final : public Sentence {
public:
    void setInsertable(bool insertable) {
        insertable_ = insertable;
    }

    void setVid(int64_t vid) {
        vid_ = vid;
    }

    void setUpdateList(UpdateList *items) {
        updateItems_.reset(items);
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    std::string toString() const override;

private:
    bool                                        insertable_{false};
    int64_t                                     vid_{0};
    std::unique_ptr<UpdateList>                 updateItems_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
};


class UpdateEdgeSentence final : public Sentence {
public:
    void setInsertable(bool insertable) {
        insertable_ = insertable;
    }

    void setSrcId(int64_t srcid) {
        srcid_ = srcid;
    }

    void setDstId(int64_t dstid) {
        dstid_ = dstid;
    }

    void setRank(int64_t rank) {
        rank_ = rank;
    }

    void setUpdateList(UpdateList *items) {
        updateItems_.reset(items);
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    std::string toString() const override;

private:
    bool                                        insertable_{false};
    int64_t                                     srcid_{0};
    int64_t                                     dstid_{0};
    int64_t                                     rank_{0};
    std::unique_ptr<UpdateList>                 updateItems_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
};

class DeleteVertexSentence final : public Sentence {
public:
    explicit DeleteVertexSentence(SourceNodeList *srcNodeList) {
        srcNodeList_.reset(srcNodeList);
        kind_ = Kind::kDeleteVertex;
    }

    const SourceNodeList* srcNodeLists() const {
        return srcNodeList_.get();
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<SourceNodeList>            srcNodeList_;
    std::unique_ptr<WhereClause>               whereClause_;
};

class EdgeList final {
public:
    void addEdge(int64_t srcid, int64_t dstid) {
        edges_.emplace_back(std::make_pair(srcid, dstid));
    }

    const std::vector<std::pair<int64_t, int64_t>>& edges() const {
        return edges_;
    }

    std::string toString() const;

private:
    std::vector<std::pair<int64_t, int64_t>>    edges_;
};

class DeleteEdgeSentence final : public Sentence {
public:
    explicit DeleteEdgeSentence(EdgeList *edgeList) {
        edgeList_.reset(edgeList);
        kind_ = Kind::kDeleteEdge;
    }

    const EdgeList* edgeList() const {
        return edgeList_.get();
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<EdgeList>                   edgeList_;
    std::unique_ptr<WhereClause>                whereClause_;
};

}  // namespace nebula

#endif  // PARSER_MUTATESENTENCES_H_
