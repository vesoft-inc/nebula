/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ADMIN_JOB_VALIDATOR_H_
#define VALIDATOR_ADMIN_JOB_VALIDATOR_H_

#include "validator/Validator.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class AdminJobValidator final : public Validator {
public:
    AdminJobValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<AdminJobSentence*>(sentence);
        if (!requireSpace()) {
            setNoSpaceRequired();
        }
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    bool requireSpace() const {
        switch (sentence_->getOp()) {
            case meta::cpp2::AdminJobOp::ADD:
                switch (sentence_->getCmd()) {
                    case meta::cpp2::AdminCmd::REBUILD_TAG_INDEX:
                    case meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX:
                    case meta::cpp2::AdminCmd::REBUILD_FULLTEXT_INDEX:
                    case meta::cpp2::AdminCmd::STATS:
                    case meta::cpp2::AdminCmd::COMPACT:
                    case meta::cpp2::AdminCmd::FLUSH:
                        return true;
                    case meta::cpp2::AdminCmd::DATA_BALANCE:
                    case meta::cpp2::AdminCmd::DOWNLOAD:
                    case meta::cpp2::AdminCmd::INGEST:
                    case meta::cpp2::AdminCmd::UNKNOWN:
                        return false;
                }
                break;
            case meta::cpp2::AdminJobOp::SHOW_All:
            case meta::cpp2::AdminJobOp::SHOW:
            case meta::cpp2::AdminJobOp::STOP:
            case meta::cpp2::AdminJobOp::RECOVER:
                return false;
        }
        return false;
    }

private:
    AdminJobSentence               *sentence_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // VALIDATOR_ADMIN_JOB_VALIDATOR_H_
