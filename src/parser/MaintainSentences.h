/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_MAINTAINSENTENCES_H_
#define PARSER_MAINTAINSENTENCES_H_

#include <interface/gen-cpp2/common_types.h>
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

    std::string* name() const {
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

    std::string* name() const {
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

    std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};


class AlterTagOptItem final {
public:
    enum OptionType : uint8_t {
        ADD = 0x01,
        SET = 0x02,
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
    AlterTagSentence(std::string *name, AlterTagOptList *opts) {
        name_.reset(name);
        opts_.reset(opts);
        kind_ = Kind::kAlterTag;
    }

    std::string toString() const override;

    std::string* name() const {
        return name_.get();
    }

    std::vector<AlterTagOptItem*> tagOptList() const {
        return opts_->alterTagItems();
    }

private:
    std::unique_ptr<std::string>        name_;
    std::unique_ptr<AlterTagOptList>    opts_;
};


class AlterEdgeSentence final : public Sentence {
public:
    AlterEdgeSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
        kind_ = Kind::kAlterEdge;
    }

    std::string toString() const override;

    std::string* name() const {
        return name_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};


class DescribeTagSentence final : public Sentence {
public:
    explicit DescribeTagSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kDescribeTag;
    }

    std::string toString() const override;

    std::string* name() const {
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
