/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

class InsertVertexSentence final : public Sentence {
public:
    InsertVertexSentence(int64_t id, std::string *vertex, PropertyList *props,
                         ValueList *values, bool overwritable = true) {
        id_ = id;
        vertex_.reset(vertex);
        properties_.reset(props);
        values_.reset(values);
        overwritable_ = overwritable;
        kind_ = Kind::kInsertVertex;
    }

    bool overwritable() const {
        return overwritable_;
    }

    int64_t id() const {
        return id_;
    }

    std::string* vertex() const {
        return vertex_.get();
    }

    std::vector<std::string*> properties() const {
        return properties_->properties();
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const override;

private:
    bool                                        overwritable_{true};
    int64_t                                     id_;
    std::unique_ptr<std::string>                vertex_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<ValueList>                  values_;
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

    void setSrcId(int64_t srcid) {
        srcid_ = srcid;
    }

    int64_t srcid() const {
        return srcid_;
    }

    void setDstId(int64_t dstid) {
        dstid_ = dstid;
    }

    int64_t dstid() const {
        return dstid_;
    }

    void setRank(int64_t rank) {
        rank_ = rank;
    }

    int64_t rank() const {
        return rank_;
    }

    void setEdge(std::string *edge) {
        edge_.reset(edge);
    }

    std::string* edge() const {
        return edge_.get();
    }

    void setProps(PropertyList *props) {
        properties_.reset(props);
    }

    std::vector<std::string*> properties() const {
        return properties_->properties();
    }

    void setValues(ValueList *values) {
        values_.reset(values);
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const override;

private:
    bool                                        overwritable_{true};
    int64_t                                     srcid_{0};
    int64_t                                     dstid_{0};
    int64_t                                     rank_{0};
    std::unique_ptr<std::string>                edge_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<ValueList>                  values_;
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

}   // namespace nebula

#endif  // PARSER_MUTATESENTENCES_H_
