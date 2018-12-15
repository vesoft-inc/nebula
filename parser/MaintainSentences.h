/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_MAINTAINSENTENCES_H_
#define PARSER_MAINTAINSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace vesoft {

class ColumnSpecification final {
public:
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


class DefineTagSentence final : public Sentence {
public:
    DefineTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
        kind_ = Kind::kDefineTag;
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


class DefineEdgeSentence final : public Sentence {
public:
    DefineEdgeSentence(std::string *name,
                       std::string *src,
                       std::string *dst,
                       ColumnSpecificationList *columns) {
        name_.reset(name);
        src_.reset(src);
        dst_.reset(dst);
        columns_.reset(columns);
        kind_ = Kind::kDefineEdge;
    }

    std::string toString() const override;

    std::string* name() const {
        return name_.get();
    }

    std::string* src() const {
        return src_.get();
    }

    std::string* dst() const {
        return dst_.get();
    }

    std::vector<ColumnSpecification*> columnSpecs() const {
        return columns_->columnSpecs();
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<std::string>                src_;
    std::unique_ptr<std::string>                dst_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};


class AlterTagSentence final : public Sentence {
public:
    AlterTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
        kind_ = Kind::kAlterTag;
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

}   // namespace vesoft

#endif  // PARSER_MAINTAINSENTENCES_H_
