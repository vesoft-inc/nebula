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
    explicit ColumnSpecification(std::string *name) {
        name_.reset(name);
    }

    ColumnSpecification(ColumnType type, std::string *name) {
        type_ = type;
        name_.reset(name);
    }

    ColumnSpecification(ColumnType type, std::string *name, int64_t ttl) {
        hasTTL_ = true;
        ttl_ = ttl;
        type_ = type;
        name_.reset(name);
    }

    bool hasTTL() const {
        return hasTTL_;
    }

    int64_t ttl() const {
        return ttl_;
    }

    ColumnType type() const {
        return type_;
    }

    const std::string* name() const {
        return name_.get();
    }

private:
    bool                                        hasTTL_{false};
    int64_t                                     ttl_;
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


class CreateTagSentence final : public Sentence {
public:
    CreateTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
        kind_ = Kind::kCreateTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};


class CreateEdgeSentence final : public Sentence {
public:
    CreateEdgeSentence(std::string *name,
                       ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
        kind_ = Kind::kCreateEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
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

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

    OptionType getOptType() {
        return optType_;
    }

    std::string getOptTypeStr() const {
        return typeid(optType_).name();
    }

    nebula::meta::cpp2::AlterSchemaOp toType();

    std::string toString() const;

private:
    OptionType                                  optType_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
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
    AlterTagSentence(std::string *name, AlterSchemaOptList *opts) {
        name_.reset(name);
        opts_.reset(opts);
        kind_ = Kind::kAlterTag;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<AlterSchemaOptItem*> schemaOptList() const {
        return opts_->alterSchemaItems();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<AlterSchemaOptList>         opts_;
};


class AlterEdgeSentence final : public Sentence {
public:
    AlterEdgeSentence(std::string *name, AlterSchemaOptList *opts) {
        name_.reset(name);
        opts_.reset(opts);
        kind_ = Kind::kAlterEdge;
    }

    std::string toString() const override;

    const std::string* name() const {
        return name_.get();
    }

    std::vector<AlterSchemaOptItem*> schemaOptList() const {
        return opts_->alterSchemaItems();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<AlterSchemaOptList>         opts_;
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

class RemoveTagSentence final : public Sentence {
public:
    explicit RemoveTagSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kRemoveTag;
    }

    std::string toString() const override;

    std::string* name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>                name_;
};


class RemoveEdgeSentence final : public Sentence {
public:
    explicit RemoveEdgeSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kRemoveEdge;
    }

    std::string toString() const override;

    std::string* name() const {
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
