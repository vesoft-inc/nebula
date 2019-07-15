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
         if (nullptr == properties_) {
             return {};
         }
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
    VertexRowItem(Expression *id, ValueList *values) {
        id_.reset(id);
        values_.reset(values);
    }

    Expression* id() const {
        return id_.get();
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 id_;
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
    EdgeRowItem(Expression *srcid, Expression *dstid, ValueList *values) {
        srcid_.reset(srcid);
        dstid_.reset(dstid);
        values_.reset(values);
    }

    EdgeRowItem(Expression *srcid, Expression *dstid, int64_t rank, ValueList *values) {
        srcid_.reset(srcid);
        dstid_.reset(dstid);
        rank_ = rank;
        values_.reset(values);
    }

    auto srcid() const {
        return srcid_.get();
    }

    auto dstid() const {
        return dstid_.get();
    }

    auto rank() const {
        return rank_;
    }

    std::vector<Expression*> values() const {
        return values_->values();
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 srcid_;
    std::unique_ptr<Expression>                 dstid_;
    int64_t                                     rank_;
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
        if (nullptr == properties_) {
            return {};
        }
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

    void setVid(Expression *vid) {
        vid_.reset(vid);
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
    std::unique_ptr<Expression>                 vid_;
    std::unique_ptr<UpdateList>                 updateItems_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
};


class UpdateEdgeSentence final : public Sentence {
public:
    void setInsertable(bool insertable) {
        insertable_ = insertable;
    }

    void setSrcId(Expression *srcid) {
        srcid_.reset(srcid);
    }

    void setDstId(Expression *dstid) {
        dstid_.reset(dstid);
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
    std::unique_ptr<Expression>                 srcid_;
    std::unique_ptr<Expression>                 dstid_;
    int64_t                                     rank_{0};
    std::unique_ptr<UpdateList>                 updateItems_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
};


class DeleteVertexSentence final : public Sentence {
public:
    explicit DeleteVertexSentence(VertexIDList *vidList) {
        vidList_.reset(vidList);
        kind_ = Kind::kDeleteVertex;
    }

    auto vidList() const {
        return vidList_->vidList();
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<VertexIDList>               vidList_;
    std::unique_ptr<WhereClause>                whereClause_;
};


class EdgeList final {
public:
    void addEdge(Expression *srcid, Expression *dstid) {
        edges_.emplace_back(srcid, dstid);
    }

    const auto& edges() const {
        return edges_;
    }

    std::string toString() const;

private:
    using EdgeItem = std::pair<std::unique_ptr<Expression>, std::unique_ptr<Expression>>;
    std::vector<EdgeItem>                       edges_;
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

class IngestSentence final : public Sentence {
public:
    explicit IngestSentence(std::string *path) {
        path_.reset(path);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string> path_;
};

class DownloadSentence final : public Sentence {
public:
    DownloadSentence() {
        kind_ = Kind::kDownload;
    }

    const std::string* host() const {
        return host_.get();
    }

    void setHost(std::string *host) {
        host_.reset(host);
    }

    const int32_t port() const {
        return port_;
    }

    void setPort(int32_t port) {
        port_ = port;
    }

    const std::string* path() const {
        return path_.get();
    }

    void setPath(std::string *path) {
        path_.reset(path);
    }

    const std::string* localPath() const {
        return localPath_.get();
    }

    void setLocalPath(std::string *localPath) {
        localPath_.reset(localPath);
    }

    void setUrl(std::string *url) {
        static std::string hdfsPrefix = "hdfs://";
        if (url->find(hdfsPrefix) != 0) {
            LOG(ERROR) << "URL should start with " << hdfsPrefix;
            delete url;
            return;
        }

        std::string u = url->substr(hdfsPrefix.size(), url->size());
        std::vector<folly::StringPiece> tokens;
        folly::split(":", u, tokens);
        if (tokens.size() == 2) {
            host_ = std::make_unique<std::string>(tokens[0]);
            int32_t position = tokens[1].find_first_of("/");
            if (position != -1) {
                try {
                    port_ = folly::to<int32_t>(tokens[1].toString().substr(0, position).c_str());
                } catch (const std::exception& ex) {
                    LOG(ERROR) << "URL's port parse failed: " << *url;
                }
                path_ = std::make_unique<std::string>(
                            tokens[1].toString().substr(position, tokens[1].size()));
            } else {
                LOG(ERROR) << "URL Parse Failed: " << *url;
            }
        } else {
            LOG(ERROR) << "URL Parse Failed: " << *url;
        }
        delete url;
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                host_;
    int32_t                                     port_;
    std::unique_ptr<std::string>                path_;
    std::unique_ptr<std::string>                localPath_;
};

}  // namespace nebula

#endif  // PARSER_MUTATESENTENCES_H_
