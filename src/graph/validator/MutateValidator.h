/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VALIDATOR_MUTATEVALIDATOR_H_
#define GRAPH_VALIDATOR_MUTATEVALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"
#include "interface/gen-cpp2/storage_types.h"
#include "parser/MutateSentences.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {
class InsertVerticesValidator final : public Validator {
 public:
  InsertVerticesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateTags(const std::vector<VertexTagItem*>& tagItems);

  Status validateValues(const std::vector<VertexRowItem*>& rows);

  AstContext* getAstContext() override { return insertCtx_.get(); }

 private:
  using TagSchema = std::shared_ptr<const meta::SchemaProviderIf>;
  std::vector<std::pair<TagID, TagSchema>> schemas_;
  uint16_t propSize_{0};

  std::unique_ptr<InsertVerticesContext> insertCtx_;
};

class InsertEdgesValidator final : public Validator {
 public:
  InsertEdgesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateEdgeName();

  Status validateValues();

  AstContext* getAstContext() override { return insertCtx_.get(); }

 private:
  EdgeType edgeType_{-1};

  std::unique_ptr<InsertEdgesContext> insertCtx_;
};

class DeleteVerticesValidator final : public Validator {
 public:
  DeleteVerticesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  std::string buildVIds();

  Status toPlan() override;

 private:
  GraphSpaceID spaceId_{-1};
  // From ConstantExpression
  std::vector<Value> vertices_;
  // From InputPropertyExpression or InputPropertyExpression
  Expression* vidRef_{nullptr};
  std::vector<EdgeType> edgeTypes_;
  std::vector<std::string> edgeNames_;
  std::vector<EdgeKeyRef*> edgeKeyRefs_;
};

class DeleteTagsValidator final : public Validator {
 public:
  DeleteTagsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  std::string buildVIds();

  Status toPlan() override;

 private:
  GraphSpaceID spaceId_{-1};
  // From ConstantExpression
  std::vector<Value> vertices_;
  // From InputPropertyExpression or InputPropertyExpression
  Expression* vidRef_{nullptr};
  std::vector<TagID> tagIds_;
};

class DeleteEdgesValidator final : public Validator {
 public:
  DeleteEdgesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

  Status checkInput();

  Status buildEdgeKeyRef(const std::vector<EdgeKey*>& edgeKeys, const EdgeType edgeType);

 private:
  // From InputPropertyExpression, ConstantExpression will covert to
  // InputPropertyExpression
  std::vector<EdgeKeyRef*> edgeKeyRefs_;
  std::string edgeKeyVar_;
  ExpressionProps exprProps_;
};

class UpdateValidator : public Validator {
 public:
  explicit UpdateValidator(Sentence* sentence, QueryContext* context, bool isEdge = false)
      : Validator(sentence, context) {
    sentence_ = static_cast<UpdateBaseSentence*>(sentence);
    isEdge_ = isEdge;
  }

  virtual ~UpdateValidator() {}

 protected:
  Status initProps();

  Status getCondition();

  Status getReturnProps();

  Status getUpdateProps();

 private:
  Status checkAndResetSymExpr(Expression* inExpr,
                              const std::string& symName,
                              std::string& encodeStr);

  Expression* rewriteSymExpr(Expression* expr,
                             const std::string& sym,
                             bool& hasWrongType,
                             bool isEdge = false);

 protected:
  UpdateBaseSentence* sentence_;
  bool insertable_{false};
  GraphSpaceID spaceId_{-1};
  std::vector<std::string> returnProps_;
  std::vector<std::string> yieldColNames_;
  std::string condition_;
  std::vector<storage::cpp2::UpdatedProp> updatedProps_;
  std::string name_;
  bool isEdge_{false};
};

class UpdateVertexValidator final : public UpdateValidator {
 public:
  UpdateVertexValidator(Sentence* sentence, QueryContext* context)
      : UpdateValidator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  Value vId_;
  TagID tagId_{-1};
};

class UpdateEdgeValidator final : public UpdateValidator {
 public:
  UpdateEdgeValidator(Sentence* sentence, QueryContext* context)
      : UpdateValidator(sentence, context, true) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  Value srcId_;
  Value dstId_;
  EdgeRanking rank_{0};
  EdgeType edgeType_{-1};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_MUTATEVALIDATOR_H_
