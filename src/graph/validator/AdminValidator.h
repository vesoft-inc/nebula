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
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"

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

    bool checkTSIndex(const std::vector<meta::cpp2::FTClient>& clients,
                      const std::string& index);

private:
    meta::cpp2::SpaceDesc              spaceDesc_;
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

class AddListenerValidator final : public Validator {
public:
    AddListenerValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class RemoveListenerValidator final : public Validator {
public:
    RemoveListenerValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowListenerValidator final : public Validator {
public:
    ShowListenerValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
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

class ShowCharsetValidator final : public Validator {
public:
    ShowCharsetValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowCollationValidator final : public Validator {
public:
    ShowCollationValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowConfigsValidator final : public Validator {
public:
    ShowConfigsValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class SetConfigValidator final : public Validator {
public:
    SetConfigValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    meta::cpp2::ConfigModule                 module_;
    std::string                              name_;
    Value                                    value_;
};

class GetConfigValidator final : public Validator {
public:
    GetConfigValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    meta::cpp2::ConfigModule                 module_;
    std::string                              name_;
};

class ShowStatusValidator final : public Validator {
public:
    ShowStatusValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowTSClientsValidator final : public Validator {
public:
    ShowTSClientsValidator(Sentence* sentence, QueryContext* context)
        :Validator(sentence, context) {
            setNoSpaceRequired();
        }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class SignInTSServiceValidator final : public Validator {
public:
    SignInTSServiceValidator(Sentence* sentence, QueryContext* context)
        :Validator(sentence, context) {
            setNoSpaceRequired();
        }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class SignOutTSServiceValidator final : public Validator {
public:
    SignOutTSServiceValidator(Sentence* sentence, QueryContext* context)
        :Validator(sentence, context) {
            setNoSpaceRequired();
        }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowSessionsValidator final : public Validator {
public:
    ShowSessionsValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override {
        return Status::OK();
    }

    Status toPlan() override;
};

class GetSessionValidator final : public Validator {
public:
    GetSessionValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override {
        return Status::OK();
    }

    Status toPlan() override;

private:
    SessionID       sessionId_{0};
};

class ShowQueriesValidator final : public Validator {
public:
    ShowQueriesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class KillQueryValidator final : public Validator {
public:
    KillQueryValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_ADMINVALIDATOR_H_
