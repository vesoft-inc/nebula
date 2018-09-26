/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_MAINTAINSENTENCES_H_
#define PARSER_MAINTAINSENTENCES_H_

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

    std::vector<std::unique_ptr<ColumnSpecification>> columns_;
};

class DefineTagSentence final : public Sentence {
public:
    DefineTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class DefineEdgeSentence final : public Sentence {
public:
    DefineEdgeSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class AlterTagSentence final : public Sentence {
public:
    AlterTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class AlterEdgeSentence final : public Sentence {
public:
    AlterEdgeSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

}   // namespace vesoft

#endif  // PARSER_MAINTAINSENTENCES_H_
