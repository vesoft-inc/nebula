/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_MAINTAINSENTENCES_H_
#define PARSER_MAINTAINSENTENCES_H_

#include <interface/gen-cpp2/common_types.h>
#include <interface/gen-cpp2/meta_types.h>
#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class ColumnSpecification final {
public:
    using Value = Expression;

    ColumnSpecification(ColumnType type, std::string *name) {
        type_ = type;
        name_.reset(name);
    }

    ColumnType type() const {
        return type_;
    }

    const std::string* name() const {
        return name_.get();
    }

    void setValue(Value* expr) {
        defaultExpr_.reset(DCHECK_NOTNULL(expr));
    }

    Status MUST_USE_RESULT prepare() {
        if (hasDefaultValue()) {
            return defaultExpr_->prepare();
        }
        return Status::Error();
    }

    void setContext(ExpressionContext* ctx) {
        if (defaultExpr_ != nullptr) {
            defaultExpr_->setContext(ctx);
        }
    }

    OptVariantType getDefault(Getters& getter) {
        auto r = defaultExpr_->eval(getter);
        if (!r.ok()) {
            return std::move(r).status();
        }
        return std::move(r).value();
    }

    bool hasDefaultValue() {
        return defaultExpr_ != nullptr;
    }

private:
    ColumnType                                  type_;
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<Value>                      defaultExpr_{nullptr};
};


class ColumnSpecificationList final {
public:
    ColumnSpecificationList() = default;
    void addColumn(ColumnSpecification *column) {
        columns_.emplace_back(column);
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        std::vector<ColumnSpecification*> result;
        result.resize(columns_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(columns_.begin(), columns_.end(), result.begin(), get);
        return result;
    }

private:
    std::vector<std::unique_ptr<ColumnSpecification>> columns_;
};


class ColumnNameList final {
public:
    ColumnNameList() = default;

    void addColumn(std::string *column) {
        columns_.emplace_back(column);
    }

    std::vector<std::string*> columnNames() const {
        std::vector<std::string*> result;
        result.resize(columns_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(columns_.begin(), columns_.end(), result.begin(), get);
        return result;
    }

private:
    std::vector<std::unique_ptr<std::string>> columns_;
};


class SchemaPropItem final {
public:
    using Value = boost::variant<int64_t, bool, std::string>;

    enum PropType : uint8_t {
        TTL_DURATION,
        TTL_COL
    };

    SchemaPropItem(PropType op, int64_t val) {
        propType_ = op;
        propValue_ = val;
    }

    SchemaPropItem(PropType op, bool val) {
        propType_ = op;
        propValue_ = val;
    }

    SchemaPropItem(PropType op, std::string val) {
        propType_ = op;
        propValue_ = std::move(val);
    }

    StatusOr<int64_t> getTtlDuration() {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "Ttl_duration value illegal: " << propValue_;
            return Status::Error("Ttl_duration value illegal");
        }
    }

    StatusOr<std::string> getTtlCol() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Ttl_col value illegal: " << propValue_;
            return Status::Error("Ttl_col value illegal");
        }
    }

    PropType getPropType() {
        return propType_;
    }

    std::string toString() const;

private:
    int64_t asInt() {
        return boost::get<int64_t>(propValue_);
    }

    const std::string& asString() {
        return boost::get<std::string>(propValue_);
    }

    bool asBool() {
        switch (propValue_.which()) {
            case 0:
                return asInt() != 0;
            case 1:
                return boost::get<bool>(propValue_);
            case 2:
                return asString().empty();
            default:
                DCHECK(false);
        }
        return false;
    }

    bool isInt() {
        return propValue_.which() == 0;
    }

    bool isBool() {
        return propValue_.which() == 1;
    }

    bool isString() {
        return propValue_.which() == 2;
    }

private:
    Value        propValue_;
    PropType     propType_;
};


class SchemaPropList final {
public:
    void addOpt(SchemaPropItem *item) {
        items_.emplace_back(item);
    }

    std::vector<SchemaPropItem*> getProps() const {
        std::vector<SchemaPropItem*> result;
        result.resize(items_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(items_.begin(), items_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<SchemaPropItem>>    items_;
};


class CreateTagSentence final : public CreateSentence {
public:
    CreateTagSentence(std::string *name,
                      ColumnSpecificationList *columns,
                      SchemaPropList *schemaProps,
                      bool ifNotExists)
        : CreateSentence(ifNotExists) {
        name_.reset(name);
        columns_.reset(columns);
        schemaProps_.reset(schemaProps);
        kind_ = Kind::kCreateTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    std::vector<SchemaPropItem*> getSchemaProps() const {
        return schemaProps_->getProps();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
    std::unique_ptr<SchemaPropList>             schemaProps_;
};


class CreateEdgeSentence final : public CreateSentence {
public:
    CreateEdgeSentence(std::string *name,
                       ColumnSpecificationList *columns,
                       SchemaPropList *schemaProps,
                       bool ifNotExists)
        : CreateSentence(ifNotExists) {
        name_.reset(name);
        columns_.reset(columns);
        schemaProps_.reset(schemaProps);
        kind_ = Kind::kCreateEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    std::vector<SchemaPropItem*> getSchemaProps() const {
        return schemaProps_->getProps();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
    std::unique_ptr<SchemaPropList>             schemaProps_;
};


class AlterSchemaOptItem final {
public:
    enum OptionType : uint8_t {
        ADD = 0x01,
        CHANGE = 0x02,
        DROP = 0x03
    };

    AlterSchemaOptItem(OptionType op, ColumnSpecificationList *columns) {
        optType_ = op;
        columns_.reset(columns);
    }

    AlterSchemaOptItem(OptionType op, ColumnNameList *colNames) {
        optType_ = op;
        colNames_.reset(colNames);
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    std::vector<std::string*> columnNames() const {
        return colNames_->columnNames();
    }

    OptionType getOptType() {
        return optType_;
    }

    nebula::meta::cpp2::AlterSchemaOp toType();

    std::string toString() const;

private:
    OptionType                                  optType_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
    std::unique_ptr<ColumnNameList>             colNames_;
};


class AlterSchemaOptList final {
public:
    AlterSchemaOptList() = default;
    void addOpt(AlterSchemaOptItem *item) {
        alterSchemaItems_.emplace_back(item);
    }

    std::vector<AlterSchemaOptItem*> alterSchemaItems() const {
        std::vector<AlterSchemaOptItem*> result;
        result.resize(alterSchemaItems_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(alterSchemaItems_.begin(), alterSchemaItems_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<AlterSchemaOptItem>>    alterSchemaItems_;
};


class AlterTagSentence final : public Sentence {
public:
    AlterTagSentence(std::string *name,
                     AlterSchemaOptList *opts,
                     SchemaPropList *schemaProps) {
        name_.reset(name);
        opts_.reset(opts);
        schemaProps_.reset(schemaProps);
        kind_ = Kind::kAlterTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<AlterSchemaOptItem*> getSchemaOpts() const {
        return opts_->alterSchemaItems();
    }

    std::vector<SchemaPropItem*> getSchemaProps() const {
        return schemaProps_->getProps();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<AlterSchemaOptList>         opts_;
    std::unique_ptr<SchemaPropList>             schemaProps_;
};


class AlterEdgeSentence final : public Sentence {
public:
    AlterEdgeSentence(std::string *name,
                      AlterSchemaOptList *opts,
                      SchemaPropList *schemaProps) {
        name_.reset(name);
        opts_.reset(opts);
        schemaProps_.reset(schemaProps);
        kind_ = Kind::kAlterEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<AlterSchemaOptItem*> getSchemaOpts() const {
        return opts_->alterSchemaItems();
    }

    std::vector<SchemaPropItem*> getSchemaProps() const {
        return schemaProps_->getProps();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<AlterSchemaOptList>         opts_;
    std::unique_ptr<SchemaPropList>             schemaProps_;
};


class DescribeTagSentence final : public Sentence {
public:
    explicit DescribeTagSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kDescribeTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};


class DescribeEdgeSentence final : public Sentence {
public:
    explicit DescribeEdgeSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kDescribeEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};


class DropTagSentence final : public DropSentence {
public:
    explicit DropTagSentence(std::string *name, bool ifExists) : DropSentence(ifExists) {
        name_.reset(name);
        kind_ = Kind::kDropTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};


class DropEdgeSentence final : public DropSentence {
public:
    explicit DropEdgeSentence(std::string *name, bool ifExists) : DropSentence(ifExists) {
        name_.reset(name);
        kind_ = Kind::kDropEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};


class CreateTagIndexSentence final : public CreateSentence {
public:
    CreateTagIndexSentence(std::string *indexName,
                           std::string *tagName,
                           ColumnNameList *columns,
                           bool ifNotExists)
        : CreateSentence(ifNotExists) {
        indexName_.reset(indexName);
        tagName_.reset(tagName);
        columns_.reset(columns);
        kind_ = Kind::kCreateTagIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

    const std::string* tagName() const {
        return tagName_.get();
    }

    std::vector<std::string> names() const {
        std::vector<std::string> result;
        auto columnNames = columns_->columnNames();
        result.resize(columnNames.size());
        auto get = [] (auto ptr) { return *ptr; };
        std::transform(columnNames.begin(), columnNames.end(), result.begin(), get);
        return result;
    }

private:
    std::unique_ptr<std::string>                indexName_;
    std::unique_ptr<std::string>                tagName_;
    std::unique_ptr<ColumnNameList>             columns_;
};


class CreateEdgeIndexSentence final : public CreateSentence {
public:
    CreateEdgeIndexSentence(std::string *indexName,
                            std::string *edgeName,
                            ColumnNameList *columns,
                            bool ifNotExists)
        : CreateSentence(ifNotExists) {
        indexName_.reset(indexName);
        edgeName_.reset(edgeName);
        columns_.reset(columns);
        kind_ = Kind::kCreateEdgeIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

    const std::string* edgeName() const {
        return edgeName_.get();
    }

    std::vector<std::string> names() const {
        std::vector<std::string> result;
        auto columnNames = columns_->columnNames();
        result.resize(columnNames.size());
        auto get = [] (auto ptr) { return *ptr; };
        std::transform(columnNames.begin(), columnNames.end(), result.begin(), get);
        return result;
    }

private:
    std::unique_ptr<std::string>                indexName_;
    std::unique_ptr<std::string>                edgeName_;
    std::unique_ptr<ColumnNameList>             columns_;
};


class DescribeTagIndexSentence final : public Sentence {
public:
    explicit DescribeTagIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kDescribeTagIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};


class DescribeEdgeIndexSentence final : public Sentence {
public:
    explicit DescribeEdgeIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kDescribeEdgeIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};


class DropTagIndexSentence final : public DropSentence {
public:
    explicit DropTagIndexSentence(std::string *indexName, bool ifExists) : DropSentence(ifExists) {
        indexName_.reset(indexName);
        kind_ = Kind::kDropTagIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};


class DropEdgeIndexSentence final : public DropSentence {
public:
    explicit DropEdgeIndexSentence(std::string *indexName, bool ifExists) : DropSentence(ifExists) {
        indexName_.reset(indexName);
        kind_ = Kind::kDropEdgeIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};


class RebuildTagIndexSentence final : public Sentence {
public:
    explicit RebuildTagIndexSentence(std::string *indexName, bool isOffline) {
        indexName_.reset(indexName);
        isOffline_ = isOffline;
        kind_ = Kind::kRebuildTagIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

    bool isOffline() {
        return isOffline_;
    }

private:
    std::unique_ptr<std::string>                indexName_;
    bool                                        isOffline_;
};


class RebuildEdgeIndexSentence final : public Sentence {
public:
    explicit RebuildEdgeIndexSentence(std::string *indexName, bool isOffline) {
        indexName_.reset(indexName);
        isOffline_ = isOffline;
        kind_ = Kind::kRebuildEdgeIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

    bool isOffline() {
        return isOffline_;
    }

private:
    std::unique_ptr<std::string>                indexName_;
    bool                                        isOffline_;
};

}   // namespace nebula

#endif  // PARSER_MAINTAINSENTENCES_H_
