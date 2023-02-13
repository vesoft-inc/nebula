/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_ADMINVALIDATOR_H_
#define GRAPH_VALIDATOR_ADMINVALIDATOR_H_

#include "clients/meta/MetaClient.h"
#include "graph/validator/Validator.h"
#include "parser/AdminSentences.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {
class CreateSpaceValidator final : public Validator {
 public:
  CreateSpaceValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

  bool checkTSIndex(const std::vector<meta::cpp2::ServiceClient>& clients,
                    const std::string& index);

 private:
  meta::cpp2::SpaceDesc spaceDesc_;
  bool ifNotExist_;
};

class CreateSpaceAsValidator final : public Validator {
 public:
  CreateSpaceAsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::string oldSpaceName_;
  std::string newSpaceName_;
};

class AlterSpaceValidator final : public Validator {
 public:
  AlterSpaceValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    noSpaceRequired_ = true;
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DescSpaceValidator final : public Validator {
 public:
  DescSpaceValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowSpacesValidator final : public Validator {
 public:
  ShowSpacesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropSpaceValidator final : public Validator {
 public:
  DropSpaceValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ClearSpaceValidator final : public Validator {
 public:
  ClearSpaceValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowCreateSpaceValidator final : public Validator {
 public:
  ShowCreateSpaceValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status checkPermission() override;

  Status toPlan() override;
};

class CreateSnapshotValidator final : public Validator {
 public:
  CreateSnapshotValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropSnapshotValidator final : public Validator {
 public:
  DropSnapshotValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowSnapshotsValidator final : public Validator {
 public:
  ShowSnapshotsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class AddListenerValidator final : public Validator {
 public:
  AddListenerValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class RemoveListenerValidator final : public Validator {
 public:
  RemoveListenerValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowListenerValidator final : public Validator {
 public:
  ShowListenerValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class AddHostsValidator final : public Validator {
 public:
  AddHostsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropHostsValidator final : public Validator {
 public:
  DropHostsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowHostsValidator final : public Validator {
 public:
  ShowHostsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowMetaLeaderValidator final : public Validator {
 public:
  ShowMetaLeaderValidator(Sentence* sentence, QueryContext* ctx) : Validator(sentence, ctx) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowPartsValidator final : public Validator {
 public:
  ShowPartsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowCharsetValidator final : public Validator {
 public:
  ShowCharsetValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowCollationValidator final : public Validator {
 public:
  ShowCollationValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowConfigsValidator final : public Validator {
 public:
  ShowConfigsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class SetConfigValidator final : public Validator {
 public:
  SetConfigValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  meta::cpp2::ConfigModule module_;
  std::string name_;
  Value value_;
};

class GetConfigValidator final : public Validator {
 public:
  GetConfigValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  meta::cpp2::ConfigModule module_;
  std::string name_;
};

class ShowStatusValidator final : public Validator {
 public:
  ShowStatusValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowServiceClientsValidator final : public Validator {
 public:
  ShowServiceClientsValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class SignInServiceValidator final : public Validator {
 public:
  SignInServiceValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class SignOutServiceValidator final : public Validator {
 public:
  SignOutServiceValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowSessionsValidator final : public Validator {
 public:
  ShowSessionsValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;
  Status toPlan() override;
};

class KillSessionValidator final : public Validator {
 public:
  KillSessionValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;
  Status toPlan() override;
};

class GetSessionValidator final : public Validator {
 public:
  GetSessionValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override {
    return Status::OK();
  }

  Status toPlan() override;

 private:
  SessionID sessionId_{0};
};

class ShowQueriesValidator final : public Validator {
 public:
  ShowQueriesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class KillQueryValidator final : public Validator {
 public:
  KillQueryValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_ADMINVALIDATOR_H_
