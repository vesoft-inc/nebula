/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ADMINVALIDATOR_H_
#define VALIDATOR_ADMINVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"
#include "clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
class CreateSpaceValidator final : public Validator {
public:
    CreateSpaceValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<CreateSpaceSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    CreateSpaceSentence               *sentence_{nullptr};
    meta::SpaceDesc                    spaceDesc_;
    bool                               ifNotExist_;
};

class DescSpaceValidator final : public Validator {
public:
    DescSpaceValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DescribeSpaceSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DescribeSpaceSentence                   *sentence_{nullptr};
    std::string                              spaceName_;
};

}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_ADMINVALIDATOR_H_
