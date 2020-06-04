/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MAINTAINVALIDATOR_H_
#define VALIDATOR_MAINTAINVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
class CreateTagValidator final : public Validator {
public:
    CreateTagValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
        sentence_ = static_cast<CreateTagSentence*>(sentence);
    }

private:
    Status validateImpl() override;
    Status toPlan() override;

private:
    CreateTagSentence               *sentence_{nullptr};
    std::string                      tagName_;
    meta::cpp2::Schema               schema_;
    bool                             ifNotExist_;
};

class CreateEdgeValidator final : public Validator {
public:
    CreateEdgeValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
        sentence_ = static_cast<CreateEdgeSentence*>(sentence);
    }

private:
    Status validateImpl() override;
    Status toPlan() override;

private:
    CreateEdgeSentence               *sentence_{nullptr};
    std::string                       edgeName_;
    meta::cpp2::Schema                schema_;
    bool                              ifNotExist_;
};

class DescTagValidator final : public Validator {
public:
    DescTagValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DescribeTagSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DescribeTagSentence                   *sentence_{nullptr};
    std::string                            tagName_;
};

class DescEdgeValidator final : public Validator {
public:
    DescEdgeValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DescribeEdgeSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DescribeEdgeSentence               *sentence_{nullptr};
    std::string                         edgeName_;
};

}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_MAINTAINVALIDATOR_H_
