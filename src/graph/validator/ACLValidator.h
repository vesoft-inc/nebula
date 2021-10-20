/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VALIDATOR_ACLVALIDATOR_H_
#define GRAPH_VALIDATOR_ACLVALIDATOR_H_

#include "graph/validator/Validator.h"
#include "parser/UserSentences.h"

namespace nebula {
namespace graph {

class CreateUserValidator final : public Validator {
 public:
  CreateUserValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class DropUserValidator final : public Validator {
 public:
  DropUserValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class UpdateUserValidator final : public Validator {
 public:
  UpdateUserValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowUsersValidator final : public Validator {
 public:
  ShowUsersValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ChangePasswordValidator final : public Validator {
 public:
  ChangePasswordValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class GrantRoleValidator final : public Validator {
 public:
  GrantRoleValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class RevokeRoleValidator final : public Validator {
 public:
  RevokeRoleValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;
};

class ShowRolesInSpaceValidator final : public Validator {
 public:
  ShowRolesInSpaceValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status checkPermission() override;

  Status toPlan() override;

  GraphSpaceID targetSpaceId_{-1};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_ACLVALIDATOR_H_
