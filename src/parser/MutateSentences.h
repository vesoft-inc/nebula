/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_MUTATESENTENCES_H_
#define PARSER_MUTATESENTENCES_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "parser/Clauses.h"
#include "parser/EdgeKey.h"
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

    bool isDefaultPropNames() const {
        return defaultPropNames_;
    }

    void setDefaultPropNames() {
        defaultPropNames_ = true;
    }

    std::vector<std::string*> properties() const {
        if (nullptr == properties_) {
            return {};
        }
        return properties_->properties();
    }

private:
    bool                                       defaultPropNames_{false};
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

    const std::vector<Expression*> &values() const {
        return values_;
    }

private:
    std::vector<Expression*>    values_;
};


class VertexRowItem final {
public:
    VertexRowItem(Expression *id, ValueList *values) {
        id_ = id;
        values_.reset(values);
    }

    Expression* id() const {
        return id_;
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const;

private:
    Expression*                 id_{nullptr};
    std::unique_ptr<ValueList>  values_;
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


class InsertVerticesSentence final : public Sentence {
public:
    InsertVerticesSentence(VertexTagList *tagList,
                           VertexRowList *rows,
                           bool ifNotExists) {
        tagList_.reset(tagList);
        rows_.reset(rows);
        ifNotExists_ = ifNotExists;
        kind_ = Kind::kInsertVertices;
    }

    auto tagItems() const {
        return tagList_->tagItems();
    }

    std::vector<VertexRowItem*> rows() const {
        return rows_->rows();
    }

    std::string toString() const override;

    bool isIfNotExists() const {
        return ifNotExists_;
    }

private:
    bool                                        ifNotExists_{false};
    std::unique_ptr<VertexTagList>              tagList_;
    std::unique_ptr<VertexRowList>              rows_;
};


class EdgeRowItem final {
public:
    EdgeRowItem(Expression *srcid, Expression *dstid, ValueList *values) {
        srcid_ = srcid;
        dstid_ = dstid;
        values_.reset(values);
    }

    EdgeRowItem(Expression *srcid, Expression *dstid, int64_t rank, ValueList *values) {
        srcid_ = srcid;
        dstid_ = dstid;
        rank_ = rank;
        values_.reset(values);
    }

    auto srcid() const {
        return srcid_;
    }

    auto dstid() const {
        return dstid_;
    }

    auto rank() const {
        return rank_;
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const;

private:
    Expression *srcid_{nullptr};
    Expression *dstid_{nullptr};
    EdgeRanking rank_{0};
    std::unique_ptr<ValueList> values_;
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


class InsertEdgesSentence final : public Sentence {
public:
    explicit InsertEdgesSentence(std::string* edge, EdgeRowList* rows, bool ifNotExists)
        : Sentence(Kind::kInsertEdges) {
        edge_.reset(edge);
        rows_.reset(rows);
        ifNotExists_ = ifNotExists;
    }

    const std::string* edge() const {
        return edge_.get();
    }

    void setProps(PropertyList *props) {
        properties_.reset(props);
    }

    std::vector<std::string*> properties() const {
        if (nullptr == properties_) {
            return {};
        }
        return properties_->properties();
    }

    std::vector<EdgeRowItem*> rows() const {
        return rows_->rows();
    }

    bool isIfNotExists() const {
        return ifNotExists_;
    }

    void setDefaultPropNames() {
        isDefaultPropNames_ = true;
    }

    bool isDefaultPropNames() const {
        return isDefaultPropNames_;
    }

    std::string toString() const override;

private:
    bool                                        isDefaultPropNames_{false};
    bool                                        ifNotExists_{false};
    std::unique_ptr<std::string>                edge_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<EdgeRowList>                rows_;
};


class UpdateItem final {
public:
    UpdateItem(std::string *field, Expression *value) {
        fieldStr_.reset(field);
        value_ = value;
    }

    UpdateItem(Expression *field, Expression *value) {
        fieldExpr_ = field;
        value_ = value;
    }

    std::string* getFieldName() const {
        return fieldStr_.get();
    }

    const Expression* getFieldExpr() const {
        return fieldExpr_;
    }

    const Expression* value() const {
        return value_;
    }

    std::string toString() const;

    StatusOr<std::string> toEvaledString() const;

private:
    std::unique_ptr<std::string> fieldStr_;
    Expression *fieldExpr_{nullptr};
    Expression *value_{nullptr};
};

class UpdateList final {
public:
    UpdateList() = default;
    ~UpdateList() = default;

    void addItem(UpdateItem *item) {
        items_.emplace_back(item);
    }

    std::vector<UpdateItem*> items() const {
        std::vector<UpdateItem*> result;
        result.reserve(items_.size());
        for (auto &item : items_) {
             result.emplace_back(item.get());
        }
        return result;
    }

    std::string toString() const;

    StatusOr<std::string> toEvaledString() const;

private:
    std::vector<std::unique_ptr<UpdateItem>>    items_;
};

class UpdateBaseSentence : public Sentence {
public:
    UpdateBaseSentence(UpdateList *updateList,
                       WhenClause *whenClause,
                       YieldClause *yieldClause,
                       std::string* name,
                       bool isInsertable = false) {
        updateList_.reset(updateList);
        whenClause_.reset(whenClause);
        yieldClause_.reset(yieldClause);
        name_.reset(name);
        insertable_ = isInsertable;
    }

    virtual ~UpdateBaseSentence() = default;

    bool getInsertable() const {
        return insertable_;
    }

    const UpdateList* updateList() const {
        return updateList_.get();
    }

    const WhenClause* whenClause() const {
        return whenClause_.get();
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    const std::string* getName() const {
        return name_.get();
    }

protected:
    bool                                        insertable_{false};
    std::unique_ptr<UpdateList>                 updateList_;
    std::unique_ptr<WhenClause>                 whenClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
    std::unique_ptr<std::string>                name_;
};

class UpdateVertexSentence final : public UpdateBaseSentence {
public:
    UpdateVertexSentence(Expression *vid,
                         std::string *tagName,
                         UpdateList *updateList,
                         WhenClause *whenClause,
                         YieldClause *yieldClause,
                         bool isInsertable = false)
        : UpdateBaseSentence(updateList, whenClause, yieldClause, tagName, isInsertable) {
        kind_ = Kind::kUpdateVertex;
        vid_ = vid;
    }

    UpdateVertexSentence(Expression *vid,
                         UpdateList *updateList,
                         WhenClause *whenClause,
                         YieldClause *yieldClause,
                         bool isInsertable = false)
        : UpdateBaseSentence(updateList, whenClause, yieldClause, nullptr, isInsertable) {
        kind_ = Kind::kUpdateVertex;
        vid_ = vid;
    }

    ~UpdateVertexSentence() {}

    bool getInsertable() const {
        return insertable_;
    }

    Expression* getVid() const {
        return vid_;
    }

    const UpdateList* updateList() const {
        return updateList_.get();
    }

    const WhenClause* whenClause() const {
        return whenClause_.get();
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    Expression*                 vid_{nullptr};
};


class UpdateEdgeSentence final : public UpdateBaseSentence {
public:
    UpdateEdgeSentence(Expression *srcId,
                       Expression *dstId,
                       int64_t rank,
                       std::string *edgeName,
                       UpdateList *updateList,
                       WhenClause *whenClause,
                       YieldClause *yieldClause,
                       bool isInsertable = false)
        : UpdateBaseSentence(updateList, whenClause, yieldClause, edgeName, isInsertable) {
        kind_ = Kind::kUpdateEdge;
        srcId_ = srcId;
        dstId_ = dstId;
        rank_ = rank;
    }

    Expression* getSrcId() const {
        return srcId_;
    }

    Expression* getDstId() const {
        return dstId_;
    }

    int64_t getRank() const {
        return rank_;
    }

    std::string toString() const override;

private:
    Expression *srcId_{nullptr};
    Expression *dstId_{nullptr};
    int64_t rank_{0L};
};


class DeleteVerticesSentence final : public Sentence {
public:
    explicit DeleteVerticesSentence(VertexIDList *vidList)
        : Sentence(Kind::kDeleteVertices),
          vertices_(new VerticesClause(vidList)) {
    }

    explicit DeleteVerticesSentence(Expression *ref)
        : Sentence(Kind::kDeleteVertices),
          vertices_(new VerticesClause(ref)) {
    }

    const VerticesClause* vertices() const {
        return vertices_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<VerticesClause> vertices_;
};


class DeleteEdgesSentence final : public Sentence {
public:
    DeleteEdgesSentence(std::string *edge, EdgeKeys *keys) {
        edge_.reset(edge);
        edgeKeys_.reset(keys);
        kind_ = Kind::kDeleteEdges;
    }

    DeleteEdgesSentence(std::string *edge, EdgeKeyRef *ref) {
        edge_.reset(edge);
        edgeKeyRef_.reset(ref);
        kind_ = Kind::kDeleteEdges;
    }

    const std::string* edge() const {
        return edge_.get();
    }

    EdgeKeys* edgeKeys() const {
        return edgeKeys_.get();
    }

    EdgeKeyRef* edgeKeyRef() const {
        return edgeKeyRef_.get();
    }

    bool isRef() const {
        return edgeKeyRef_ != nullptr;
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                edge_;
    std::unique_ptr<EdgeKeys>                   edgeKeys_;
    std::unique_ptr<EdgeKeyRef>                 edgeKeyRef_;
};


class DownloadSentence final : public Sentence {
public:
    DownloadSentence() {
        kind_ = Kind::kDownload;
    }

    const std::string* host() const {
        return host_.get();
    }

    int32_t port() const {
        return port_;
    }

    void setPort(int32_t port) {
        port_ = port;
    }

    const std::string* path() const {
        return path_.get();
    }

    void setUrl(std::string& url) {
        static std::string hdfsPrefix = "hdfs://";
        if (url.find(hdfsPrefix) != 0) {
            LOG(ERROR) << "URL should start with " << hdfsPrefix;
            return;
        }

        std::string u = url.substr(hdfsPrefix.size(), url.size());
        std::vector<folly::StringPiece> tokens;
        folly::split(":", u, tokens);
        if (tokens.size() == 2) {
            host_ = std::make_unique<std::string>(tokens[0]);
            int32_t position = tokens[1].find_first_of("/");
            if (position != -1) {
                try {
                    port_ = folly::to<int32_t>(tokens[1].toString().substr(0, position).c_str());
                } catch (const std::exception& ex) {
                    LOG(ERROR) << "URL's port parse failed: " << url;
                    return;
                }
                path_ = std::make_unique<std::string>(
                            tokens[1].toString().substr(position, tokens[1].size()));
            } else {
                LOG(ERROR) << "URL Parse Failed: " << url;
            }
        } else {
            LOG(ERROR) << "URL Parse Failed: " << url;
        }
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                host_;
    int32_t                                     port_;
    std::unique_ptr<std::string>                path_;
};

class IngestSentence final : public Sentence {
public:
    IngestSentence() {
        kind_ = Kind::kIngest;
    }

    std::string toString() const override;
};

}  // namespace nebula
#endif  // PARSER_MUTATESENTENCES_H_
