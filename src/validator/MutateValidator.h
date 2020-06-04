/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MUTATEVALIDATOR_H
#define VALIDATOR_MUTATEVALIDATOR_H
#include "common/base/Base.h"
#include "validator/Validator.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/MutateSentences.h"

namespace nebula {
namespace graph {
class InsertVerticesValidator final : public Validator {
public:
    InsertVerticesValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
        sentence_ = static_cast<InsertVerticesSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareVertices();

private:
    using TagSchema = std::shared_ptr<const meta::SchemaProviderIf>;
    InsertVerticesSentence                                *sentence_{nullptr};
    std::vector<VertexRowItem*>                            rows_;
    std::unordered_map<TagID, std::vector<std::string>>    tagPropNames_;
    std::vector<std::pair<TagID, TagSchema>>               schemas_;
    uint16_t                                               propSize_{0};
    bool                                                   overwritable_{false};
    std::vector<storage::cpp2::NewVertex>                  vertices_;
};

class InsertEdgesValidator final : public Validator {
public:
    InsertEdgesValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<InsertEdgesSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareEdges();

private:
    InsertEdgesSentence                              *sentence_{nullptr};
    bool                                              overwritable_{true};
    EdgeType                                          edgeType_{0};
    std::shared_ptr<const meta::SchemaProviderIf>     schema_;
    std::vector<std::string>                          propNames_;
    std::vector<EdgeRowItem*>                         rows_;
    std::vector<storage::cpp2::NewEdge>               edges_;
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_MUTATEVALIDATOR_H
