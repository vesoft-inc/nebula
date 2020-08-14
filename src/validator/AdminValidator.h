/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ADMINVALIDATOR_H_
#define VALIDATOR_ADMINVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
class CreateSpaceValidator final : public Validator {
public:
    CreateSpaceValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    meta::SpaceDesc                    spaceDesc_;
    bool                               ifNotExist_;
};

class DescSpaceValidator final : public Validator {
public:
    DescSpaceValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowSpacesValidator final : public Validator {
public:
    ShowSpacesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class DropSpaceValidator final : public Validator {
public:
    DropSpaceValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
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
    DropSnapshotValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowSnapshotsValidator final : public Validator {
public:
    ShowSnapshotsValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowHostsValidator final : public Validator {
public:
    ShowHostsValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowPartsValidator final : public Validator {
public:
    ShowPartsValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_ADMINVALIDATOR_H_
