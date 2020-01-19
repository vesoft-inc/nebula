/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateSpaceExecutor.h"
#include "common/charset/Charset.h"
<<<<<<< HEAD
#include "graph/GraphFlags.h"
=======
>>>>>>> support utf8mb4 charset

namespace nebula {
namespace graph {

CreateSpaceExecutor::CreateSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
    : Executor(ectx, "create_space") {
    sentence_ = static_cast<CreateSpaceSentence*>(sentence);
}


Status CreateSpaceExecutor::prepare() {
<<<<<<< HEAD
    spaceDesc_ = meta::SpaceDesc();
    spaceDesc_.spaceName_ = std::move(*(sentence_->spaceName()));
    Status retStatus;
    StatusOr<std::string> retStatusOr;
    std::string result;
    auto* charsetInfo = ectx()->getCharsetInfo();
    for (auto &item : sentence_->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM: {
                spaceDesc_.partNum_ = item->get_partition_num();
                if (spaceDesc_.partNum_ <= 0) {
                    return Status::Error("Partition_num value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::REPLICA_FACTOR: {
                spaceDesc_.replicaFactor_ = item->get_replica_factor();
                if (spaceDesc_.replicaFactor_ <= 0) {
                    return Status::Error("Replica_factor value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::CHARSET: {
                result = item->get_charset();
                folly::toLowerAscii(result);
                retStatus = charsetInfo->isSupportCharset(result);
                if (!retStatus.ok()) {
                    return retStatus;
                }
                spaceDesc_.charsetName_ = std::move(result);
                break;
            }
            case SpaceOptItem::COLLATE: {
                result = item->get_collate();
                folly::toLowerAscii(result);
                retStatus = charsetInfo->isSupportCollate(result);
                if (!retStatus.ok()) {
                    return retStatus;
                }
                spaceDesc_.collationName_ = std::move(result);
                break;
            }
        }
    }

    // if charset and collate are not specified, set default value
    if (!spaceDesc_.charsetName_.empty() && !spaceDesc_.collationName_.empty()) {
        retStatus = charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                                                        spaceDesc_.collationName_);
        if (!retStatus.ok()) {
            return retStatus;
        }
    } else if (!spaceDesc_.charsetName_.empty()) {
        retStatusOr = charsetInfo->getDefaultCollationbyCharset(spaceDesc_.charsetName_);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.collationName_ = std::move(retStatusOr.value());
    } else if (!spaceDesc_.collationName_.empty()) {
        retStatusOr = charsetInfo->getCharsetbyCollation(spaceDesc_.collationName_);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.charsetName_ = std::move(retStatusOr.value());
    }

    if (spaceDesc_.charsetName_.empty() && spaceDesc_.collationName_.empty()) {
        std::string charsetName = FLAGS_default_charset;
        folly::toLowerAscii(charsetName);
        retStatus = charsetInfo->isSupportCharset(charsetName);
        if (!retStatus.ok()) {
            return retStatus;
        }

        std::string collateName = FLAGS_default_collate;
        folly::toLowerAscii(collateName);
        retStatus = charsetInfo->isSupportCollate(collateName);
        if (!retStatus.ok()) {
            return retStatus;
        }

        spaceDesc_.charsetName_ = std::move(charsetName);
        spaceDesc_.collationName_ = std::move(collateName);

        retStatus = charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                                                        spaceDesc_.collationName_);
        if (!retStatus.ok()) {
            return retStatus;
        }
    }

=======
    spaceMeta_.spaceName_ = std::move(*(sentence_->spaceName()));
    Status retStatus;
    StatusOr<std::string> retStatusOr;
    for (auto &item : sentence_->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM:
                spaceMeta_.partNum_ = item->get_partition_num();
                if (spaceMeta_.partNum_ <= 0) {
                    return Status::Error("Partition_num value should be greater than zero");
                }
                break;
            case SpaceOptItem::REPLICA_FACTOR:
                spaceMeta_.replicaFactor_ = item->get_replica_factor();
                if (spaceMeta_.replicaFactor_ <= 0) {
                    return Status::Error("Replica_factor value should be greater than zero");
                }
                break;
            case SpaceOptItem::CHARSET:
                spaceMeta_.charsetName_ = item->get_charset();
                folly::toLowerAscii(spaceMeta_.charsetName_);
                retStatus = CharsetInfo::isSupportCharset(spaceMeta_.charsetName_);
                if (!retStatus.ok()) {
                    return retStatus;
                }
                break;
            case SpaceOptItem::COLLATE:
                spaceMeta_.collationName_ = item->get_collate();
                folly::toLowerAscii(spaceMeta_.collationName_);
                retStatus = CharsetInfo::isSupportCollate(spaceMeta_.collationName_);
                if (!retStatus.ok()) {
                    return retStatus;
                }
                break;
        }
    }

    // if charset and collate are not specified, set in meta
    if (!spaceMeta_.charsetName_.empty() && !spaceMeta_.collationName_.empty()) {
        retStatus = CharsetInfo::charsetAndCollateMatch(spaceMeta_.charsetName_,
                                                        spaceMeta_.collationName_);
        if (!retStatus.ok()) {
            return retStatus;
        }
    } else if (!spaceMeta_.charsetName_.empty()) {
        retStatusOr = CharsetInfo::getDefaultCollationbyCharset(spaceMeta_.charsetName_);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceMeta_.collationName_ = std::move(retStatusOr.value());
    } else if (!spaceMeta_.collationName_.empty()) {
        retStatusOr = CharsetInfo::getCharsetbyCollation(spaceMeta_.collationName_);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceMeta_.charsetName_ = std::move(retStatusOr.value());
    }

>>>>>>> support utf8mb4 charset
    return Status::OK();
}


void CreateSpaceExecutor::execute() {
<<<<<<< HEAD
    auto future = ectx()->getMetaClient()->createSpace(spaceDesc_, sentence_->isIfNotExist());
=======
    auto future = ectx()->getMetaClient()->createSpace(spaceMeta_, sentence_->isIfNotExist());
>>>>>>> support utf8mb4 charset
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Create space `%s' failed: %s.",
<<<<<<< HEAD
                                   spaceDesc_.spaceName_.c_str(),
=======
                                   spaceMeta_.spaceName_.c_str(),
>>>>>>> support utf8mb4 charset
                                   resp.status().toString().c_str()));
            return;
        }
        auto spaceId = std::move(resp).value();
        if (spaceId <= 0) {
            doError(Status::Error("Create space failed"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Create space `%s' exception: %s.",
<<<<<<< HEAD
                                        spaceDesc_.spaceName_.c_str(), e.what().c_str());
=======
                                        spaceMeta_.spaceName_.c_str(), e.what().c_str());
>>>>>>> support utf8mb4 charset
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
