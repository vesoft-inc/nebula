/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_PIPEVALIDATOR_H_
#define VALIDATOR_PIPEVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class PipeValidator final : public Validator {
public:
    PipeValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        auto pipeSentence = static_cast<PipedSentence*>(sentence_);
        auto left = pipeSentence->left();
        lValidator_ = makeValidator(left, qctx_);

        auto right = pipeSentence->right();
        rValidator_ = makeValidator(right, qctx_);

        if (lValidator_->noSpaceRequired() && rValidator_->noSpaceRequired()) {
            setNoSpaceRequired();
        }
    }

private:
    Status validateImpl() override;

    /**
     * Connect the execution plans for the left and right subtrees.
     * For example: Go FROM id1 OVER e1 YIELD e1._dst AS id | GO FROM $-.id OVER e2;
     * The plan of left subtree's would be:
     *  Project(_dst) -> GetNeighbor(id1, e1)
     * and the right would be:
     *  Project(_dst) -> GetNeighbor(id2, e2)
     * After connecting, it would be:
     *  Project(_dst) -> GetNeighbor(id2, e2) ->
                Project(_dst) -> GetNeighbor(id1, e1)
     */
    Status toPlan() override;

private:
    std::unique_ptr<Validator>  lValidator_;
    std::unique_ptr<Validator>  rValidator_;
};
}  // namespace graph
}  // namespace nebula
#endif
