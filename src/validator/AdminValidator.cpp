/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/AdminValidator.h"

#include "common/base/Base.h"
#include "common/charset/Charset.h"
#include "util/SchemaUtil.h"
#include "parser/MaintainSentences.h"
#include "service/GraphFlags.h"
#include "planner/Admin.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status CreateSpaceValidator::validateImpl() {
    auto status = Status::OK();
    spaceDesc_ = meta::SpaceDesc();
    spaceDesc_.spaceName_ = std::move(*(sentence_->spaceName()));
    StatusOr<std::string> retStatusOr;
    std::string result;
    auto* charsetInfo = qctx_->getCharsetInfo();
    for (auto &item : sentence_->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM: {
                spaceDesc_.partNum_ = item->getPartitionNum();
                if (spaceDesc_.partNum_ <= 0) {
                    return Status::Error("Partition_num value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::REPLICA_FACTOR: {
                spaceDesc_.replicaFactor_ = item->getReplicaFactor();
                if (spaceDesc_.replicaFactor_ <= 0) {
                    return Status::Error("Replica_factor value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::VID_SIZE: {
                spaceDesc_.vidSize_ = item->getVidSize();
                if (spaceDesc_.vidSize_ < 0 ||
                        spaceDesc_.vidSize_ > std::numeric_limits<int32_t>::max()) {
                    return Status::Error("Vid_size value is incorrect");
                }
                break;
            }
            case SpaceOptItem::CHARSET: {
                result = item->getCharset();
                folly::toLowerAscii(result);
                NG_RETURN_IF_ERROR(charsetInfo->isSupportCharset(result));
                spaceDesc_.charsetName_ = std::move(result);
                break;
            }
            case SpaceOptItem::COLLATE: {
                result = item->getCollate();
                folly::toLowerAscii(result);
                NG_RETURN_IF_ERROR(charsetInfo->isSupportCollate(result));
                spaceDesc_.collationName_ = std::move(result);
                break;
            }
        }
    }

    // if charset and collate are not specified, set default value
    if (!spaceDesc_.charsetName_.empty() && !spaceDesc_.collationName_.empty()) {
        NG_RETURN_IF_ERROR(charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                    spaceDesc_.collationName_));
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
        NG_RETURN_IF_ERROR(charsetInfo->isSupportCharset(charsetName));

        std::string collateName = FLAGS_default_collate;
        folly::toLowerAscii(collateName);
        NG_RETURN_IF_ERROR(charsetInfo->isSupportCollate(collateName));

        spaceDesc_.charsetName_ = std::move(charsetName);
        spaceDesc_.collationName_ = std::move(collateName);

        NG_RETURN_IF_ERROR(charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                    spaceDesc_.collationName_));
    }

    ifNotExist_ = sentence_->isIfNotExist();
    return status;
}

Status CreateSpaceValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = CreateSpace::make(plan, spaceDesc_, ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DescSpaceValidator::validateImpl() {
    spaceName_ = *sentence_->spaceName();
    return Status::OK();
}

Status DescSpaceValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = DescSpace::make(plan, spaceName_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
