/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_SETVALIDATOR_H_
#define VALIDATOR_SETVALIDATOR_H_

#include "validator/Validator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class SetValidator final : public Validator {
public:
    SetValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        auto setSentence = static_cast<SetSentence *>(sentence_);
        lValidator_ = makeValidator(setSentence->left(), qctx_);
        rValidator_ = makeValidator(setSentence->right(), qctx_);

        if (lValidator_->noSpaceRequired() && rValidator_->noSpaceRequired()) {
            setNoSpaceRequired();
        }
    }

private:
    Status validateImpl() override;

    /**
     * Merge the starting node of the execution plan
     * for the left and right subtrees.
     * For example: Go FROM id1 OVER e1 UNION GO FROM id2 OVER e2;
     * The plan of left subtree's would be:
     *  Project(_dst) -> GetNeighbor(id1, e1)
     * and the right would be:
     *  Project(_dst) -> GetNeighbor(id2, e2)
     * After merging, it would be:
     *  Union -> Project(_dst) -> GetNeighbor(id1, e1) -|
     *        |-> Project(_dst) -> GetNeighbor(id2, e2) -> End
     */
    Status toPlan() override;

private:
    std::unique_ptr<Validator>  lValidator_;
    std::unique_ptr<Validator>  rValidator_;
};

}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_SETVALIDATOR_H_
