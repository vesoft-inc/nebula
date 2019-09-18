/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_INDEXSENTENCES_H
#define PARSER_INDEXSENTENCES_H

#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class CreateTagIndexSentence final : public Sentence {
public:
    explicit CreateTagIndexSentence(std::string *indexName,
                                     std::string *tagName,
                                     std::string *field) {
        // TODO multi fields
        indexName_.reset(indexName);
        tagName_.reset(tagName);
        field_.reset(field);
        kind_ = Kind::kCreateTagIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
        return indexName_.get();
    }

    std::string* tagName() const {
        return tagName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
    std::unique_ptr<std::string>                tagName_;
    std::unique_ptr<std::string>                field_;
};

class CreateEdgeIndexSentence final : public Sentence {
public:
    explicit CreateEdgeIndexSentence(std::string *indexName,
                                      std::string *edgeName,
                                      std::string *field) {
        // TODO multi fields
        indexName_.reset(indexName);
        edgeName_.reset(edgeName);
        field_.reset(field);
        kind_ = Kind::kCreateEdgeIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
        return indexName_.get();
    }

    std::string* edgeName() const {
        return edgeName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
    std::unique_ptr<std::string>                edgeName_;
    std::unique_ptr<std::string>                field_;
};

class DropTagIndexSentence final : public Sentence {
public:
    explicit DropTagIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kDropTagIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

class DropEdgeIndexSentence final : public Sentence {
public:
    explicit DropEdgeIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kDropEdgeIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

class DescribeTagIndexSentence final : public Sentence {
public:
    explicit DescribeTagIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kDescribeTagIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
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

    std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

class RebuildTagIndexSentence final : public Sentence {
public:
    explicit RebuildTagIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kRebuildTagIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

class RebuildEdgeIndexSentence final : public Sentence {
public:
    explicit RebuildEdgeIndexSentence(std::string *indexName) {
        indexName_.reset(indexName);
        kind_ = Kind::kRebuildEdgeIndex;
    }

    std::string toString() const override;

    std::string* indexName() const {
        return indexName_.get();
    }

private:
    std::unique_ptr<std::string>                indexName_;
};

}   // namespace nebula

#endif  // PARSER_INDEXSENTENCES_H

