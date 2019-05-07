/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_MAINTAINSENTENCES_H_
#define PARSER_MAINTAINSENTENCES_H_

#include <interface/gen-cpp2/common_types.h>
#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class ColumnSpecification final {
public:
    explicit ColumnSpecification(std::string *name) {
        name_.reset(name);
    }

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

private:
    ColumnType                                  type_;
    std::unique_ptr<std::string>                name_;
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


class SchemaOptItem final {
public:
    using Value = boost::variant<int64_t, bool, std::string>;

    enum OptionType : uint8_t {
        TTL_DURATION,
        TTL_COL,
        COMMENT,
        ENGINE,
        ENCRYPT,
        COMPRESS,
        CHARACTER_SET,
        COLLATE
    };

    SchemaOptItem(OptionType op, int64_t val) {
        optType_ = op;
        optValue_ = val;
    }

    SchemaOptItem(OptionType op, bool val) {
        optType_ = op;
        optValue_ = val;
    }

    SchemaOptItem(OptionType op, std::string val) {
        optType_ = op;
        optValue_ = std::move(val);
    }

    int64_t asInt() {
        return boost::get<int64_t>(optValue_);
    }

    std::string& asString() {
        return boost::get<std::string>(optValue_);
    }

    bool asBool() {
        switch (optValue_.which()) {
            case 0:
                return asInt() != 0;
            case 1:
                return boost::get<bool>(optValue_);
            case 2:
                return asString().empty();
            default:
                DCHECK(false);
        }
        return false;
    }

    bool isInt() {
        return optValue_.which() == 0;
    }

    bool isBool() {
        return optValue_.which() == 1;
    }

    bool isString() {
        return optValue_.which() == 2;
    }

    int64_t getTtlDuration() {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "Ttl_duration value illegal.";
            return 0;
        }
    }

    std::string getTtlCol() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Ttl_col value illegal.";
            return "";
        }
    }

    std::string getComment() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Comment value illegal.";
            return "";
        }
    }

    std::string getEngine() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Engine value illegal.";
            return "";
        }
    }

    std::string getEncrypt() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Encrypt value illegal.";
            return "";
        }
    }

    std::string getCompress() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Compress value illegal.";
            return "";
        }
    }

    std::string getCharacterSet() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Character set value illegal.";
            return "";
        }
    }

    std::string getCollate() {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "Collate value illegal.";
            return "";
        }
    }

    OptionType getOptType() {
        return optType_;
    }

    std::string toString() const;

 private:
    Value        optValue_;
    OptionType   optType_;
};


class SchemaOptList final {
public:
    void addOpt(SchemaOptItem *item) {
        items_.emplace_back(item);
    }

    std::vector<std::unique_ptr<SchemaOptItem>> getOpt() {
        return std::move(items_);
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<SchemaOptItem>>    items_;
};


class CreateTagSentence final : public Sentence {
public:
    CreateTagSentence(std::string *name,
                      ColumnSpecificationList *columns,
                      SchemaOptList *schemaOpts) {
        name_.reset(name);
        columns_.reset(columns);
        schemaOpts_.reset(schemaOpts);
        kind_ = Kind::kCreateTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    std::vector<std::unique_ptr<SchemaOptItem>> getSchemaOpts() {
        return schemaOpts_->getOpt();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
    std::unique_ptr<SchemaOptList>              schemaOpts_;
};


class CreateEdgeSentence final : public Sentence {
public:
    CreateEdgeSentence(std::string *name,
                       ColumnSpecificationList *columns,
                       SchemaOptList *schemaOpts) {
        name_.reset(name);
        columns_.reset(columns);
        schemaOpts_.reset(schemaOpts);
        kind_ = Kind::kCreateEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    std::vector<std::unique_ptr<SchemaOptItem>> getSchemaOpts() {
        return schemaOpts_->getOpt();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
    std::unique_ptr<SchemaOptList>              schemaOpts_;
};


class AlterTagOptItem final {
public:
    enum OptionType : uint8_t {
        ADD = 0x01,
        CHANGE = 0x02,
        DROP = 0x03
    };

    AlterTagOptItem(OptionType op, ColumnSpecificationList *columns) {
        optType_ = op;
        columns_.reset(columns);
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    OptionType getOptType() {
        return optType_;
    }

    std::string getOptTypeStr() const {
        return typeid(optType_).name();
    }

    std::string toString() const;

private:
    OptionType                                  optType_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};


class AlterTagOptList final {
public:
    AlterTagOptList() = default;
    void addOpt(AlterTagOptItem *item) {
        alterTagitems_.emplace_back(item);
    }

    std::vector<AlterTagOptItem*> alterTagItems() const {
        std::vector<AlterTagOptItem*> result;
        result.resize(alterTagitems_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(alterTagitems_.begin(), alterTagitems_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<AlterTagOptItem>>    alterTagitems_;
};


class AlterTagSentence final : public Sentence {
public:
    AlterTagSentence(std::string *name,
                     SchemaOptList *schemaOpts,
                     AlterTagOptList *opts) {
        name_.reset(name);
        schemaOpts_.reset(schemaOpts);
        opts_.reset(opts);
        kind_ = Kind::kAlterTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<AlterTagOptItem*> tagOptList() const {
        return opts_->alterTagItems();
    }

    std::vector<std::unique_ptr<SchemaOptItem>> getSchemaOpts() {
        return schemaOpts_->getOpt();
    }

private:
    std::unique_ptr<std::string>        name_;
    std::unique_ptr<AlterTagOptList>    opts_;
    std::unique_ptr<SchemaOptList>      schemaOpts_;
};


class AlterEdgeSentence final : public Sentence {
public:
    AlterEdgeSentence(std::string *name,
                      ColumnSpecificationList *columns,
                      SchemaOptList *schemaOpts) {
        name_.reset(name);
        columns_.reset(columns);
        schemaOpts_.reset(schemaOpts);
        kind_ = Kind::kAlterEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    std::vector<std::unique_ptr<SchemaOptItem>> getSchemaOpts() {
        return schemaOpts_->getOpt();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
    std::unique_ptr<SchemaOptList>              schemaOpts_;
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


class YieldSentence final : public Sentence {
 public:
     explicit YieldSentence(YieldColumns *fields) {
         yieldColumns_.reset(fields);
         kind_ = Kind::kYield;
     }

     std::vector<YieldColumn*> columns() const {
         return yieldColumns_->columns();
     }

     std::string toString() const override;

 private:
     std::unique_ptr<YieldColumns>              yieldColumns_;
};

}   // namespace nebula

#endif  // PARSER_MAINTAINSENTENCES_H_
