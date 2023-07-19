/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_MAINTAINVALIDATOR_H_
#define GRAPH_VALIDATOR_MAINTAINVALIDATOR_H_

#include "clients/meta/MetaClient.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"
#include "interface/gen-cpp2/meta_types.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class CreateTagValidator final : public Validator {
 public:
  CreateTagValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

  AstContext* getAstContext() override {
    return createCtx_.get();
  }

 private:
  Status validateImpl() override;

  std::unique_ptr<CreateSchemaContext> createCtx_;
};

class CreateEdgeValidator final : public Validator {
 public:
  CreateEdgeValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

  AstContext* getAstContext() override {
    return createCtx_.get();
  }

 private:
  Status validateImpl() override;

  std::unique_ptr<CreateSchemaContext> createCtx_;
};

class DescTagValidator final : public Validator {
 public:
  DescTagValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowCreateTagValidator final : public Validator {
 public:
  ShowCreateTagValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DescEdgeValidator final : public Validator {
 public:
  DescEdgeValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowCreateEdgeValidator final : public Validator {
 public:
  ShowCreateEdgeValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class AlterTagValidator final : public Validator {
 public:
  AlterTagValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

  AstContext* getAstContext() override {
    return alterCtx_.get();
  }

 private:
  Status validateImpl() override;

  std::unique_ptr<AlterSchemaContext> alterCtx_;
};

class AlterEdgeValidator final : public Validator {
 public:
  AlterEdgeValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

  AstContext* getAstContext() override {
    return alterCtx_.get();
  }

 private:
  Status validateImpl() override;

  std::unique_ptr<AlterSchemaContext> alterCtx_;
};

class ShowTagsValidator final : public Validator {
 public:
  ShowTagsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowEdgesValidator final : public Validator {
 public:
  ShowEdgesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropTagValidator final : public Validator {
 public:
  DropTagValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropEdgeValidator final : public Validator {
 public:
  DropEdgeValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class CreateTagIndexValidator final : public Validator {
 public:
  CreateTagIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;
  Status toPlan() override;

 private:
  std::string name_;
  std::string index_;
  std::vector<meta::cpp2::IndexFieldDef> fields_;
  bool ifNotExist_;
  std::unique_ptr<meta::cpp2::IndexParams> indexParams_;
};

class CreateEdgeIndexValidator final : public Validator {
 public:
  CreateEdgeIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;
  Status toPlan() override;

 private:
  std::string name_;
  std::string index_;
  std::vector<meta::cpp2::IndexFieldDef> fields_;
  bool ifNotExist_;
  std::unique_ptr<meta::cpp2::IndexParams> indexParams_;
};

class DropTagIndexValidator final : public Validator {
 public:
  DropTagIndexValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string indexName_;
  bool ifExist_;
};

class DropEdgeIndexValidator final : public Validator {
 public:
  DropEdgeIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string indexName_;
  bool ifExist_;
};

class DescribeTagIndexValidator final : public Validator {
 public:
  DescribeTagIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string indexName_;
};

class DescribeEdgeIndexValidator final : public Validator {
 public:
  DescribeEdgeIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string indexName_;
};

class ShowCreateTagIndexValidator final : public Validator {
 public:
  ShowCreateTagIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string indexName_;
};

class ShowCreateEdgeIndexValidator final : public Validator {
 public:
  ShowCreateEdgeIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string indexName_;
};

class ShowTagIndexesValidator final : public Validator {
 public:
  ShowTagIndexesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string name_;
};

class ShowEdgeIndexesValidator final : public Validator {
 public:
  ShowEdgeIndexesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string name_;
};

class ShowTagIndexStatusValidator final : public Validator {
 public:
  ShowTagIndexStatusValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowEdgeIndexStatusValidator final : public Validator {
 public:
  ShowEdgeIndexStatusValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class MergeZoneValidator final : public Validator {
 public:
  MergeZoneValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class RenameZoneValidator final : public Validator {
 public:
  RenameZoneValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropZoneValidator final : public Validator {
 public:
  DropZoneValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DivideZoneValidator final : public Validator {
 public:
  DivideZoneValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DescribeZoneValidator final : public Validator {
 public:
  DescribeZoneValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowZonesValidator final : public Validator {
 public:
  ShowZonesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class AddHostsIntoZoneValidator final : public Validator {
 public:
  AddHostsIntoZoneValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class CreateFTIndexValidator final : public Validator {
 public:
  CreateFTIndexValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  meta::cpp2::FTIndex index_;
  const size_t kFulltextIndexNameLength = 256;
};

class DropFTIndexValidator final : public Validator {
 public:
  DropFTIndexValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};
class ShowFTIndexesValidator final : public Validator {
 public:
  ShowFTIndexesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_MAINTAINVALIDATOR_H_
