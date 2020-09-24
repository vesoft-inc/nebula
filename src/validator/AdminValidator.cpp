/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/AdminValidator.h"

#include "common/base/Base.h"
#include "common/charset/Charset.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "parser/MaintainSentences.h"
#include "planner/Admin.h"
#include "planner/Query.h"
#include "service/GraphFlags.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {
Status CreateSpaceValidator::validateImpl() {
    auto sentence = static_cast<CreateSpaceSentence*>(sentence_);
    ifNotExist_ = sentence->isIfNotExist();
    auto status = Status::OK();
    spaceDesc_.space_name = std::move(*(sentence->spaceName()));
    StatusOr<std::string> retStatusOr;
    std::string result;
    auto* charsetInfo = qctx_->getCharsetInfo();
    for (auto &item : sentence->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM: {
                spaceDesc_.partition_num = item->getPartitionNum();
                if (spaceDesc_.partition_num <= 0) {
                    return Status::Error("Partition_num value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::REPLICA_FACTOR: {
                spaceDesc_.replica_factor = item->getReplicaFactor();
                if (spaceDesc_.replica_factor <= 0) {
                    return Status::Error("Replica_factor value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::VID_TYPE: {
                auto typeDef = item->getVidType();
                if (typeDef.type != meta::cpp2::PropertyType::INT64 &&
                        typeDef.type != meta::cpp2::PropertyType::FIXED_STRING) {
                    std::stringstream ss;
                    ss << "Only support FIXED_STRING or INT64 vid type, but was given "
                       << meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(typeDef.type);
                    return Status::Error(ss.str());
                }
                spaceDesc_.vid_type.set_type(typeDef.type);

                if (typeDef.type == meta::cpp2::PropertyType::INT64) {
                    spaceDesc_.vid_type.set_type_length(8);
                } else {
                    if (!typeDef.__isset.type_length) {
                        return Status::Error("type length is not set for fixed string type");
                    }
                    if (*typeDef.get_type_length() <= 0) {
                        return Status::Error("Vid size should be a positive number: %d",
                                             *typeDef.get_type_length());
                    }
                    spaceDesc_.vid_type.set_type_length(*typeDef.get_type_length());
                }
                break;
            }
            case SpaceOptItem::CHARSET: {
                result = item->getCharset();
                folly::toLowerAscii(result);
                NG_RETURN_IF_ERROR(charsetInfo->isSupportCharset(result));
                spaceDesc_.charset_name = std::move(result);
                break;
            }
            case SpaceOptItem::COLLATE: {
                result = item->getCollate();
                folly::toLowerAscii(result);
                NG_RETURN_IF_ERROR(charsetInfo->isSupportCollate(result));
                spaceDesc_.collate_name = std::move(result);
                break;
            }
        }
    }

    // if charset and collate are not specified, set default value
    if (!spaceDesc_.charset_name.empty() && !spaceDesc_.collate_name.empty()) {
        NG_RETURN_IF_ERROR(charsetInfo->charsetAndCollateMatch(spaceDesc_.charset_name,
                    spaceDesc_.collate_name));
    } else if (!spaceDesc_.charset_name.empty()) {
        retStatusOr = charsetInfo->getDefaultCollationbyCharset(spaceDesc_.charset_name);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.collate_name = std::move(retStatusOr.value());
    } else if (!spaceDesc_.collate_name.empty()) {
        retStatusOr = charsetInfo->getCharsetbyCollation(spaceDesc_.collate_name);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.charset_name = std::move(retStatusOr.value());
    }

    if (spaceDesc_.charset_name.empty() && spaceDesc_.collate_name.empty()) {
        std::string charsetName = FLAGS_default_charset;
        folly::toLowerAscii(charsetName);
        NG_RETURN_IF_ERROR(charsetInfo->isSupportCharset(charsetName));

        std::string collateName = FLAGS_default_collate;
        folly::toLowerAscii(collateName);
        NG_RETURN_IF_ERROR(charsetInfo->isSupportCollate(collateName));

        spaceDesc_.charset_name = std::move(charsetName);
        spaceDesc_.collate_name = std::move(collateName);

        NG_RETURN_IF_ERROR(charsetInfo->charsetAndCollateMatch(spaceDesc_.charset_name,
                    spaceDesc_.collate_name));
    }

    // add to validate context
    vctx_->addSpace(spaceDesc_.space_name);
    return status;
}

Status CreateSpaceValidator::toPlan() {
    auto *doNode = CreateSpace::make(qctx_, nullptr, std::move(spaceDesc_), ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DescSpaceValidator::validateImpl() {
    return Status::OK();
}

Status DescSpaceValidator::toPlan() {
    auto sentence = static_cast<DescribeSpaceSentence*>(sentence_);
    auto *doNode = DescSpace::make(qctx_, nullptr, *sentence->spaceName());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status ShowSpacesValidator::validateImpl() {
    return Status::OK();
}

Status ShowSpacesValidator::toPlan() {
    auto *doNode = ShowSpaces::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DropSpaceValidator::validateImpl() {
    return Status::OK();
}

Status DropSpaceValidator::toPlan() {
    auto sentence = static_cast<DropSpaceSentence*>(sentence_);
    auto *doNode = DropSpace::make(qctx_, nullptr, *sentence->spaceName(), sentence->isIfExists());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status ShowCreateSpaceValidator::validateImpl() {
    return Status::OK();
}

Status ShowCreateSpaceValidator::toPlan() {
    auto sentence = static_cast<ShowCreateSpaceSentence*>(sentence_);
    auto spaceName = *sentence->spaceName();
    auto *doNode = ShowCreateSpace::make(qctx_, nullptr, std::move(spaceName));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status CreateSnapshotValidator::validateImpl() {
    return Status::OK();
}

Status CreateSnapshotValidator::toPlan() {
    auto *doNode = CreateSnapshot::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DropSnapshotValidator::validateImpl() {
    return Status::OK();
}

Status DropSnapshotValidator::toPlan() {
    auto sentence = static_cast<DropSnapshotSentence*>(sentence_);
    auto *doNode = DropSnapshot::make(qctx_, nullptr, *sentence->name());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status ShowSnapshotsValidator::validateImpl() {
    return Status::OK();
}

Status ShowSnapshotsValidator::toPlan() {
    auto *doNode = ShowSnapshots::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status ShowHostsValidator::validateImpl() {
    return Status::OK();
}

Status ShowHostsValidator::toPlan() {
    auto *showHosts = ShowHosts::make(qctx_, nullptr);
    root_ = showHosts;
    tail_ = root_;
    return Status::OK();
}

Status ShowPartsValidator::validateImpl() {
    return Status::OK();
}

Status ShowPartsValidator::toPlan() {
    auto sentence = static_cast<ShowPartsSentence*>(sentence_);
    std::vector<PartitionID> partIds;
    if (sentence->getList() != nullptr) {
        partIds = *sentence->getList();
    }
    auto *node = ShowParts::make(qctx_,
                                 nullptr,
                                 vctx_->whichSpace().id,
                                 std::move(partIds));
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status ShowCharsetValidator::validateImpl() {
    return Status::OK();
}

Status ShowCharsetValidator::toPlan() {
    auto *node = ShowCharset::make(qctx_, nullptr);
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status ShowCollationValidator::validateImpl() {
    return Status::OK();
}

Status ShowCollationValidator::toPlan() {
    auto *node = ShowCollation::make(qctx_, nullptr);
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status ShowConfigsValidator::validateImpl() {
    return Status::OK();
}

Status ShowConfigsValidator::toPlan() {
    auto sentence = static_cast<ShowConfigsSentence*>(sentence_);
    meta::cpp2::ConfigModule module;
    auto item = sentence->configItem();
    if (item != nullptr) {
        module = item->getModule();
    } else {
        module = meta::cpp2::ConfigModule::ALL;
    }
    auto *doNode = ShowConfigs::make(qctx_, nullptr, module);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status SetConfigValidator::validateImpl() {
    auto sentence = static_cast<SetConfigSentence*>(sentence_);
    auto item = sentence->configItem();
    if (item == nullptr) {
        return Status::Error("Empty config item");
    }
    if (item->getName() != nullptr) {
        name_ = *item->getName();
    }
    name_ = *item->getName();
    module_ = item->getModule();
    auto updateItems = item->getUpdateItems();
    QueryExpressionContext ctx;
    if (updateItems == nullptr) {
        module_ = item->getModule();
        if (item->getName() != nullptr) {
            name_ = *item->getName();
        }

        if (item->getValue() != nullptr) {
            value_ = Expression::eval(item->getValue(), ctx(nullptr));
        }
    } else {
        Map configs;
        for (auto &updateItem : updateItems->items()) {
            std::string name;
            Value value;
            if (updateItem->getFieldName() == nullptr || updateItem->value() == nullptr) {
                return Status::Error("Empty item");
            }
            name = *updateItem->getFieldName();

            value = Expression::eval(const_cast<Expression*>(updateItem->value()), ctx(nullptr));

            if (value.isNull() || (!value.isNumeric() && !value.isStr() && !value.isBool())) {
                return Status::Error("Wrong value: %s", name.c_str());
            }
            configs.kvs.emplace(std::move(name), std::move(value));
        }
        value_.setMap(std::move(configs));
    }

    return Status::OK();
}

Status SetConfigValidator::toPlan() {
    auto *doNode = SetConfig::make(qctx_,
                                   nullptr,
                                   module_,
                                   std::move(name_),
                                   std::move(value_));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status GetConfigValidator::validateImpl() {
    auto sentence = static_cast<GetConfigSentence*>(sentence_);
    auto item = sentence->configItem();
    if (item == nullptr) {
        return Status::Error("Empty config item");
    }

    module_ = item->getModule();
    if (item->getName() != nullptr) {
        name_ = *item->getName();
    }
    name_ = *item->getName();
    return Status::OK();
}

Status GetConfigValidator::toPlan() {
    auto *doNode = GetConfig::make(qctx_,
                                   nullptr,
                                   module_,
                                   std::move(name_));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
