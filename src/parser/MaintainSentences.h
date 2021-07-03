/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_MAINTAINSENTENCES_H_
#define PARSER_MAINTAINSENTENCES_H_

#include <string>
#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"
#include "context/QueryExpressionContext.h"

namespace nebula {

std::ostream& operator<<(std::ostream& os, meta::cpp2::PropertyType type);

class ColumnProperty final {
public:
    using Value = std::variant<bool, Expression*, std::unique_ptr<std::string>>;

    explicit ColumnProperty(bool nullable = true)
        : v_(nullable) {}

    explicit ColumnProperty(Expression *defaultValue = nullptr)
        : v_(defaultValue) {}

    explicit ColumnProperty(std::string *comment = nullptr)
        : v_(std::unique_ptr<std::string>(comment)) {}

    bool isNullable() const {
        return std::holds_alternative<bool>(v_);
    }

    bool nullable() const {
        DCHECK(isNullable());
        return std::get<bool>(v_);
    }

    bool isDefaultValue() const {
        return std::holds_alternative<Expression*>(v_);
    }

    const auto* defaultValue() const {
        DCHECK(isDefaultValue());
        return std::get<Expression*>(v_);
    }

    bool isComment() const {
        return std::holds_alternative<std::unique_ptr<std::string>>(v_);
    }

    const auto* comment() const {
        DCHECK(isComment());
        return std::get<std::unique_ptr<std::string>>(v_).get();
    }

    std::string toString() const;

private:
    Value v_;
};

class ColumnProperties final {
public:
    ColumnProperties() = default;

    void addProperty(ColumnProperty *property) {
        properties_.emplace_back(property);
    }

    auto& properties() const {
        return properties_;
    }

    std::string toString() const {
        std::stringstream str;
        for (const auto &property : properties_) {
            str << property->toString();
            str << " ";
        }
        return str.str();
    }

private:
    std::vector<std::unique_ptr<ColumnProperty>> properties_;
};

class ColumnSpecification final {
public:
    ColumnSpecification(std::string *name,
                        meta::cpp2::PropertyType type,
                        ColumnProperties *properties,
                        int16_t typeLen = 0)
        : name_(name),
          type_(type),
          properties_(DCHECK_NOTNULL(properties)),
          typeLen_(typeLen) {}

    meta::cpp2::PropertyType type() const {
        return type_;
    }

    const std::string* name() const {
        return name_.get();
    }

    int16_t typeLen() const {
        return typeLen_;
    }

    auto& properties() const {
        return properties_;
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>                name_;
    meta::cpp2::PropertyType                    type_;
    std::unique_ptr<ColumnProperties>           properties_{nullptr};
    int16_t                                     typeLen_{0};
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

    std::string toString() const;

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

    std::string toString() const {
        std::string buf;
        buf.reserve(128);
        for (const auto &col : columns_) {
            buf += *col;
            buf += ", ";
        }
        if (!columns_.empty()) {
            buf.pop_back();
            buf.pop_back();
        }
        return buf;
    }

private:
    std::vector<std::unique_ptr<std::string>> columns_;
};


class SchemaPropItem final {
public:
    using Value = boost::variant<int64_t, bool, std::string>;

    enum PropType : uint8_t {
        TTL_DURATION,
        TTL_COL,
        COMMENT
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

    StatusOr<std::string> getComment() {
        if (propType_ == COMMENT) {
            return asString();
        } else {
            return Status::Error("Not exists comment.");
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


class IndexFieldList final {
public:
    IndexFieldList() = default;

    void addField(std::unique_ptr<meta::cpp2::IndexFieldDef> field) {
        fields_.emplace_back(std::move(field));
    }

    std::vector<meta::cpp2::IndexFieldDef*> fields() const {
        std::vector<meta::cpp2::IndexFieldDef*> result;
        result.resize(fields_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(fields_.begin(), fields_.end(), result.begin(), get);
        return result;
    }

private:
    std::vector<std::unique_ptr<meta::cpp2::IndexFieldDef>> fields_;
};


class CreateTagIndexSentence final : public CreateSentence {
public:
    CreateTagIndexSentence(std::string *indexName,
                           std::string *tagName,
                           IndexFieldList *fields,
                           bool ifNotExists,
                           std::string *comment)
        : CreateSentence(ifNotExists) {
        indexName_.reset(indexName);
        tagName_.reset(tagName);
        if (fields == nullptr) {
            fields_ = std::make_unique<IndexFieldList>();
        } else {
            fields_.reset(fields);
        }
        comment_.reset(comment);
        kind_ = Kind::kCreateTagIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

    const std::string* tagName() const {
        return tagName_.get();
    }

    std::vector<meta::cpp2::IndexFieldDef> fields() const {
        std::vector<meta::cpp2::IndexFieldDef> result;
        auto fields = fields_->fields();
        result.resize(fields.size());
        auto get = [] (auto ptr) { return *ptr; };
        std::transform(fields.begin(), fields.end(), result.begin(), get);
        return result;
    }

    const std::string* comment() const {
        return comment_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
    std::unique_ptr<std::string>                tagName_;
    std::unique_ptr<IndexFieldList>             fields_;
    std::unique_ptr<std::string>                comment_;
};


class CreateEdgeIndexSentence final : public CreateSentence {
public:
    CreateEdgeIndexSentence(std::string *indexName,
                            std::string *edgeName,
                            IndexFieldList *fields,
                            bool ifNotExists,
                            std::string *comment)
        : CreateSentence(ifNotExists) {
        indexName_.reset(indexName);
        edgeName_.reset(edgeName);
        if (fields == nullptr) {
            fields_ = std::make_unique<IndexFieldList>();
        } else {
            fields_.reset(fields);
        }
        comment_.reset(comment);
        kind_ = Kind::kCreateEdgeIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

    const std::string* edgeName() const {
        return edgeName_.get();
    }

    std::vector<meta::cpp2::IndexFieldDef> fields() const {
        std::vector<meta::cpp2::IndexFieldDef> result;
        auto fields = fields_->fields();
        result.resize(fields.size());
        auto get = [] (auto ptr) { return *ptr; };
        std::transform(fields.begin(), fields.end(), result.begin(), get);
        return result;
    }

    const std::string* comment() const {
        return comment_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
    std::unique_ptr<std::string>                edgeName_;
    std::unique_ptr<IndexFieldList>             fields_;
    std::unique_ptr<std::string>                comment_;
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


class ShowTagsSentence : public Sentence {
public:
    ShowTagsSentence() {
        kind_ = Kind::kShowTags;
    }

    std::string toString() const override;
};

class ShowEdgesSentence : public Sentence {
public:
    ShowEdgesSentence() {
        kind_ = Kind::kShowEdges;
    }

    std::string toString() const override;
};

class ShowCreateTagSentence : public Sentence {
public:
    explicit ShowCreateTagSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kShowCreateTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};

class ShowCreateEdgeSentence : public Sentence {
public:
    explicit ShowCreateEdgeSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kShowCreateEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};

class ShowTagIndexesSentence : public Sentence {
public:
    explicit ShowTagIndexesSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kShowTagIndexes;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};

class ShowEdgeIndexesSentence : public Sentence {
public:
    explicit ShowEdgeIndexesSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kShowEdgeIndexes;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};

class ShowTagIndexStatusSentence : public Sentence {
public:
    ShowTagIndexStatusSentence() {
        kind_ = Kind::kShowTagIndexStatus;
    }

    std::string toString() const override;
};

class ShowEdgeIndexStatusSentence : public Sentence {
public:
    ShowEdgeIndexStatusSentence() {
        kind_ = Kind::kShowEdgeIndexStatus;
    }

    std::string toString() const override;
};

class ShowCreateTagIndexSentence : public Sentence {
public:
    explicit ShowCreateTagIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kShowCreateTagIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

class ShowCreateEdgeIndexSentence : public Sentence {
public:
    explicit ShowCreateEdgeIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kShowCreateEdgeIndex;
    }

    std::string toString() const override;

    const std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

class ZoneNameList final {
public:
    ZoneNameList() = default;

    void addZone(std::string *zone) {
        zones_.emplace_back(zone);
    }

    std::vector<std::string> zoneNames() const {
        std::vector<std::string> result;
        result.resize(zones_.size());
        auto get = [] (auto &ptr) { return *ptr.get(); };
        std::transform(zones_.begin(), zones_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const {
        std::string buf;
        for (const auto &zone : zones_) {
            buf += *zone;
            buf += ",";
        }
        if (!zones_.empty()) {
            buf.pop_back();
        }
        return buf;
    }

private:
    std::vector<std::unique_ptr<std::string>> zones_;
};


class AddGroupSentence : public Sentence {
public:
    explicit AddGroupSentence(std::string *groupName, ZoneNameList *zoneNames) {
        groupName_.reset(groupName);
        zoneNames_.reset(zoneNames);
        kind_ = Kind::kAddGroup;
    }

    std::string toString() const override;

    const std::string* groupName() const {
        return groupName_.get();
    }

    const ZoneNameList* zoneNames() const {
        return zoneNames_.get();
    }

private:
    std::unique_ptr<std::string>                groupName_;
    std::unique_ptr<ZoneNameList>               zoneNames_;
};

class AddZoneSentence : public Sentence {
public:
    explicit AddZoneSentence(std::string *zoneName, HostList *hosts) {
        zoneName_.reset(zoneName);
        hosts_.reset(hosts);
        kind_ = Kind::kAddZone;
    }

    std::string toString() const override;

    const std::string* zoneName() const {
        return zoneName_.get();
    }

    const HostList* hosts() const {
        return hosts_.get();
    }

private:
    std::unique_ptr<std::string>                zoneName_;
    std::unique_ptr<HostList>                   hosts_;
};

class DropGroupSentence : public Sentence {
public:
    explicit DropGroupSentence(std::string *groupName) {
        groupName_.reset(groupName);
        kind_ = Kind::kDropGroup;
    }

    std::string toString() const override;

    const std::string* groupName() const {
        return groupName_.get();
    }

private:
    std::unique_ptr<std::string>                groupName_;
};

class DropZoneSentence : public Sentence {
public:
    explicit DropZoneSentence(std::string *zoneName) {
        zoneName_.reset(zoneName);
        kind_ = Kind::kDropZone;
    }

    std::string toString() const override;

    const std::string* zoneName() const {
        return zoneName_.get();
    }

private:
    std::unique_ptr<std::string>                zoneName_;
};

class DescribeGroupSentence : public Sentence {
public:
    explicit DescribeGroupSentence(std::string *groupName) {
        groupName_.reset(groupName);
        kind_ = Kind::kDescribeGroup;
    }

    std::string toString() const override;

    const std::string* groupName() const {
        return groupName_.get();
    }

private:
    std::unique_ptr<std::string>                groupName_;
};

class DescribeZoneSentence : public Sentence {
public:
    explicit DescribeZoneSentence(std::string *zoneName) {
        zoneName_.reset(zoneName);
        kind_ = Kind::kDescribeZone;
    }

    std::string toString() const override;

    const std::string* zoneName() const {
        return zoneName_.get();
    }

private:
    std::unique_ptr<std::string>                zoneName_;
};


class ListGroupsSentence : public Sentence {
public:
    ListGroupsSentence() {
        kind_ = Kind::kListGroups;
    }

    std::string toString() const override;
};

class ListZonesSentence : public Sentence {
public:
    ListZonesSentence() {
        kind_ = Kind::kListZones;
    }

    std::string toString() const override;
};

class AddZoneIntoGroupSentence : public Sentence {
public:
    AddZoneIntoGroupSentence(std::string* zoneName, std::string* groupName) {
        zoneName_.reset(zoneName);
        groupName_.reset(groupName);
        kind_ = Kind::kAddZoneIntoGroup;
    }

    const std::string* zoneName() const {
        return zoneName_.get();
    }

    const std::string* groupName() const {
        return groupName_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                zoneName_;
    std::unique_ptr<std::string>                groupName_;
};

class AddHostIntoZoneSentence : public Sentence {
public:
    explicit AddHostIntoZoneSentence(HostAddr *address, std::string* zoneName) {
        address_.reset(address);
        zoneName_.reset(zoneName);
        kind_ = Kind::kAddHostIntoZone;
    }

    const std::string* zoneName() const {
        return zoneName_.get();
    }

    const HostAddr* address() const {
        return address_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                zoneName_;
    std::unique_ptr<HostAddr>                   address_;
};

class DropZoneFromGroupSentence : public Sentence {
public:
    DropZoneFromGroupSentence(std::string* zoneName, std::string* groupName) {
        zoneName_.reset(zoneName);
        groupName_.reset(groupName);
        kind_ = Kind::kDropZoneFromGroup;
    }

    const std::string* zoneName() const {
        return zoneName_.get();
    }

    const std::string* groupName() const {
        return groupName_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                zoneName_;
    std::unique_ptr<std::string>                groupName_;
};

class DropHostFromZoneSentence : public Sentence {
public:
    explicit DropHostFromZoneSentence(HostAddr *address, std::string* zoneName) {
        address_.reset(address);
        zoneName_.reset(zoneName);
        kind_ = Kind::kDropHostFromZone;
    }

    const std::string* zoneName() const {
        return zoneName_.get();
    }

    const HostAddr* address() const {
        return address_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                zoneName_;
    std::unique_ptr<HostAddr>                   address_;
};

#ifndef FULLTEXT_INDEX_NAME_PREFIX
#define FULLTEXT_INDEX_NAME_PREFIX "nebula_"
#endif
class CreateFTIndexSentence final : public Sentence {
public:
    CreateFTIndexSentence(bool isEdge,
                          std::string* indexName,
                          std::string* schemaName,
                          NameLabelList *fields) {
        isEdge_ = isEdge;
        indexName_.reset(indexName);
        schemaName_.reset(schemaName);
        fields_.reset(fields);
        kind_ = Kind::kCreateFTIndex;
    }

    std::string toString() const override;

    bool isEdge() {
        return isEdge_;
    }
    const std::string* indexName() const {
        return indexName_.get();
    }

    const std::string* schemaName() const {
        return schemaName_.get();
    }

    std::vector<std::string> fields() const {
        std::vector<std::string> result;
        auto fields = fields_->labels();
        result.resize(fields.size());
        auto get = [] (auto ptr) { return *ptr; };
        std::transform(fields.begin(), fields.end(), result.begin(), get);
        return result;
    }

private:
    bool                             isEdge_;
    std::unique_ptr<std::string>     indexName_;
    std::unique_ptr<std::string>     schemaName_;
    std::unique_ptr<NameLabelList>   fields_;
};
class DropFTIndexSentence final : public Sentence {
public:
    explicit DropFTIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kDropFTIndex;
    }

    std::string toString() const override;

    const std::string* name() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>     indexName_;
};

class ShowFTIndexesSentence final : public Sentence {
public:
    ShowFTIndexesSentence() {
        kind_ = Kind::kShowFTIndexes;
    }
    std::string toString() const override;
};


}   // namespace nebula

#endif  // PARSER_MAINTAINSENTENCES_H_

