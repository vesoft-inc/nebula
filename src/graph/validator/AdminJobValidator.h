/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_ADMIN_JOB_VALIDATOR_H_
#define GRAPH_VALIDATOR_ADMIN_JOB_VALIDATOR_H_

#include "graph/validator/Validator.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class AdminJobValidator final : public Validator {
 public:
  AdminJobValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
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
      case meta::cpp2::JobOp::ADD:
        switch (sentence_->getJobType()) {
          // All jobs are space-level, except for the jobs that need to be refactored.
          case meta::cpp2::JobType::REBUILD_TAG_INDEX:
          case meta::cpp2::JobType::REBUILD_EDGE_INDEX:
          case meta::cpp2::JobType::REBUILD_FULLTEXT_INDEX:
          case meta::cpp2::JobType::STATS:
          case meta::cpp2::JobType::COMPACT:
          case meta::cpp2::JobType::FLUSH:
          case meta::cpp2::JobType::DATA_BALANCE:
          case meta::cpp2::JobType::LEADER_BALANCE:
          case meta::cpp2::JobType::ZONE_BALANCE:
            return true;
          // TODO: download and ingest need to be refactored to use the rpc protocol.
          // Currently they are using their own validator
          case meta::cpp2::JobType::DOWNLOAD:
          case meta::cpp2::JobType::INGEST:
          case meta::cpp2::JobType::UNKNOWN:
            return false;
        }
        break;
      case meta::cpp2::JobOp::SHOW_All:
      case meta::cpp2::JobOp::SHOW:
      case meta::cpp2::JobOp::STOP:
      case meta::cpp2::JobOp::RECOVER:
        return true;
    }
    return false;
  }

 private:
  AdminJobSentence* sentence_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_ADMIN_JOB_VALIDATOR_H_
