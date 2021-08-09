/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "validator/AdminValidator.h"

#include "common/base/Base.h"
#include "common/charset/Charset.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "parser/MaintainSentences.h"
#include "planner/plan/Admin.h"
#include "planner/plan/Query.h"
#include "service/GraphFlags.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {
Status CreateSpaceValidator::validateImpl() {
    auto sentence = static_cast<CreateSpaceSentence*>(sentence_);
    ifNotExist_ = sentence->isIfNotExist();
    auto status = Status::OK();
    spaceDesc_.set_space_name(std::move(*(sentence->spaceName())));
    if (sentence->groupName()) {
        spaceDesc_.set_group_name(std::move(*(sentence->groupName())));
    }
    StatusOr<std::string> retStatusOr;
    std::string result;
    auto* charsetInfo = qctx_->getCharsetInfo();
    auto *spaceOpts = sentence->spaceOpts();
    if (!spaceOpts || !spaceOpts->hasVidType()) {
        return Status::SemanticError("space vid_type must be specified explicitly");
    }
    for (auto &item : sentence->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM: {
                spaceDesc_.set_partition_num(item->getPartitionNum());
                if (*spaceDesc_.partition_num_ref() <= 0) {
                    return Status::SemanticError("Partition_num value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::REPLICA_FACTOR: {
                spaceDesc_.set_replica_factor(item->getReplicaFactor());
                if (*spaceDesc_.replica_factor_ref() <= 0) {
                    return Status::SemanticError(
                        "Replica_factor value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::VID_TYPE: {
                auto typeDef = item->getVidType();
                if (typeDef.type != meta::cpp2::PropertyType::INT64 &&
                    typeDef.type != meta::cpp2::PropertyType::FIXED_STRING) {
                    std::stringstream ss;
                    ss << "Only support FIXED_STRING or INT64 vid type, but was given "
                       << apache::thrift::util::enumNameSafe(typeDef.type);
                    return Status::SemanticError(ss.str());
                }
                spaceDesc_.vid_type_ref().value().set_type(typeDef.type);

                if (typeDef.type == meta::cpp2::PropertyType::INT64) {
                    spaceDesc_.vid_type_ref().value().set_type_length(8);
                } else {
                    if (!typeDef.type_length_ref().has_value()) {
                        return Status::SemanticError(
                            "type length is not set for fixed string type");
                    }
                    if (*typeDef.type_length_ref() <= 0) {
                        return Status::SemanticError("Vid size should be a positive number: %d",
                                                     *typeDef.type_length_ref());
                    }
                    spaceDesc_.vid_type_ref().value().set_type_length(*typeDef.type_length_ref());
                }
                break;
            }
            case SpaceOptItem::CHARSET: {
                result = item->getCharset();
                folly::toLowerAscii(result);
                NG_RETURN_IF_ERROR(charsetInfo->isSupportCharset(result));
                spaceDesc_.set_charset_name(std::move(result));
                break;
            }
            case SpaceOptItem::COLLATE: {
                result = item->getCollate();
                folly::toLowerAscii(result);
                NG_RETURN_IF_ERROR(charsetInfo->isSupportCollate(result));
                spaceDesc_.set_collate_name(std::move(result));
                break;
            }
            case SpaceOptItem::ATOMIC_EDGE: {
                if (item->getAtomicEdge()) {
                    spaceDesc_.set_isolation_level(meta::cpp2::IsolationLevel::TOSS);
                    // for 2.0 GA, no matter how this option set, don't use toss.
                    return ::nebula::Status::NotSupported("not support enable toss in 2.0 GA");
                } else {
                    spaceDesc_.set_isolation_level(meta::cpp2::IsolationLevel::DEFAULT);
                }
                break;
            }
            case SpaceOptItem::GROUP_NAME: {
                break;
            }
        }
    }
    // check comment
    if (sentence->comment() != nullptr) {
        spaceDesc_.set_comment(*sentence->comment());
    }

    if (sentence->groupName() != nullptr && *sentence->groupName() == "default") {
        return Status::SemanticError("Group default conflict");
    }

    // if charset and collate are not specified, set default value
    if (!(*spaceDesc_.charset_name_ref()).empty() && !(*spaceDesc_.collate_name_ref()).empty()) {
        NG_RETURN_IF_ERROR(charsetInfo->charsetAndCollateMatch(*spaceDesc_.charset_name_ref(),
                    *spaceDesc_.collate_name_ref()));
    } else if (!(*spaceDesc_.charset_name_ref()).empty()) {
        retStatusOr = charsetInfo->getDefaultCollationbyCharset(*spaceDesc_.charset_name_ref());
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.set_collate_name(std::move(retStatusOr.value()));
    } else if (!(*spaceDesc_.collate_name_ref()).empty()) {
        retStatusOr = charsetInfo->getCharsetbyCollation(*spaceDesc_.collate_name_ref());
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.set_charset_name(std::move(retStatusOr.value()));
    }

    if ((*spaceDesc_.charset_name_ref()).empty() && (*spaceDesc_.collate_name_ref()).empty()) {
        std::string charsetName = FLAGS_default_charset;
        folly::toLowerAscii(charsetName);
        NG_RETURN_IF_ERROR(charsetInfo->isSupportCharset(charsetName));

        std::string collateName = FLAGS_default_collate;
        folly::toLowerAscii(collateName);
        NG_RETURN_IF_ERROR(charsetInfo->isSupportCollate(collateName));

        spaceDesc_.set_charset_name(std::move(charsetName));
        spaceDesc_.set_collate_name(std::move(collateName));

        NG_RETURN_IF_ERROR(charsetInfo->charsetAndCollateMatch(*spaceDesc_.charset_name_ref(),
                    *spaceDesc_.collate_name_ref()));
    }

    // add to validate context
    vctx_->addSpace(*spaceDesc_.space_name_ref());
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

Status ShowCreateSpaceValidator::checkPermission() {
    auto sentence = static_cast<ShowCreateSpaceSentence*>(sentence_);
    auto spaceIdResult = qctx_->schemaMng()->toGraphSpaceID(*sentence->spaceName());
    NG_RETURN_IF_ERROR(spaceIdResult);
    auto targetSpaceId = spaceIdResult.value();
    return PermissionManager::canReadSpace(qctx_->rctx()->session(), targetSpaceId);
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

Status AddListenerValidator::validateImpl() {
    auto sentence = static_cast<AddListenerSentence*>(sentence_);
    auto hosts = sentence->listeners()->hosts();
    if (hosts.empty()) {
        return Status::SemanticError("Listener hosts should not be empty");
    }

    // check the hosts, if the hosts the same with storage, return error
    auto status = qctx_->getMetaClient()->getStorageHosts();
    if (!status.ok()) {
        return status.status();
    }

    auto storageHosts = std::move(status).value();
    for (auto &host : hosts) {
        auto iter = std::find(storageHosts.begin(), storageHosts.end(), host);
        if (iter != storageHosts.end()) {
            return Status::Error(
                    "The listener host:%s couldn't on same with storage host info",
                    host.toString().c_str());
        }
    }
    return Status::OK();
}

Status AddListenerValidator::toPlan() {
    auto sentence = static_cast<AddListenerSentence*>(sentence_);
    auto *doNode = AddListener::make(qctx_,
                                     nullptr,
                                     sentence->type(),
                                     sentence->listeners()->hosts());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status RemoveListenerValidator::validateImpl() {
    return Status::OK();
}

Status RemoveListenerValidator::toPlan() {
    auto sentence = static_cast<RemoveListenerSentence*>(sentence_);
    auto *doNode = RemoveListener::make(qctx_, nullptr, sentence->type());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status ShowListenerValidator::validateImpl() {
    return Status::OK();
}

Status ShowListenerValidator::toPlan() {
    auto *doNode = ShowListener::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status ShowHostsValidator::validateImpl() {
    return Status::OK();
}

Status ShowHostsValidator::toPlan() {
    auto sentence = static_cast<ShowHostsSentence *>(sentence_);
    auto *showHosts = ShowHosts::make(qctx_, nullptr, sentence->getType());
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
        return Status::SemanticError("Empty config item");
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
                return Status::SemanticError("Empty item");
            }
            name = *updateItem->getFieldName();

            value = Expression::eval(const_cast<Expression*>(updateItem->value()), ctx(nullptr));

            if (value.isNull() || (!value.isNumeric() && !value.isStr() && !value.isBool())) {
                return Status::SemanticError("Wrong value: `%s'", name.c_str());
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
        return Status::SemanticError("Empty config item");
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

Status ShowStatusValidator::validateImpl() {
    return Status::OK();
}

Status ShowStatusValidator::toPlan() {
    auto *node = ShowStats::make(qctx_, nullptr);
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status ShowTSClientsValidator::validateImpl() {
    return Status::OK();
}

Status ShowTSClientsValidator::toPlan() {
    auto *doNode = ShowTSClients::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status SignInTSServiceValidator::validateImpl() {
    return Status::OK();
}

Status SignInTSServiceValidator::toPlan() {
    auto sentence = static_cast<SignInTextServiceSentence*>(sentence_);
    std::vector<meta::cpp2::FTClient> clients;
    if (sentence->clients() != nullptr) {
        clients = sentence->clients()->clients();
    }
    auto *node = SignInTSService::make(qctx_, nullptr, std::move(clients));
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status SignOutTSServiceValidator::validateImpl() {
    return Status::OK();
}

Status SignOutTSServiceValidator::toPlan() {
    auto *node = SignOutTSService::make(qctx_, nullptr);
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status ShowSessionsValidator::toPlan() {
    auto sentence = static_cast<ShowSessionsSentence*>(sentence_);
    auto *node = ShowSessions::make(
            qctx_, nullptr, sentence->isSetSessionID(), sentence->getSessionID());
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status ShowQueriesValidator::validateImpl() {
    if (!inputs_.empty()) {
        return Status::SemanticError("Show queries sentence do not support input");
    }
    outputs_.emplace_back("SessionID", Value::Type::INT);
    outputs_.emplace_back("ExecutionPlanID", Value::Type::INT);
    outputs_.emplace_back("User", Value::Type::STRING);
    outputs_.emplace_back("Host", Value::Type::STRING);
    outputs_.emplace_back("StartTime", Value::Type::DATETIME);
    outputs_.emplace_back("DurationInUSec", Value::Type::INT);
    outputs_.emplace_back("Status", Value::Type::STRING);
    outputs_.emplace_back("Query", Value::Type::STRING);
    return Status::OK();
}

Status ShowQueriesValidator::toPlan() {
    auto sentence = static_cast<ShowQueriesSentence*>(sentence_);
    auto *node = ShowQueries::make(qctx_, nullptr, sentence->isAll());
    root_ = node;
    tail_ = root_;
    return Status::OK();
}

Status KillQueryValidator::validateImpl() {
    auto sentence = static_cast<KillQuerySentence *>(sentence_);
    auto *sessionExpr = sentence->sessionId();
    auto *epIdExpr = sentence->epId();
    auto sessionTypeStatus = deduceExprType(sessionExpr);
    if (!sessionTypeStatus.ok()) {
        return sessionTypeStatus.status();
    }
    if (sessionTypeStatus.value() != Value::Type::INT) {
        std::stringstream ss;
        ss << sessionExpr->toString() << ", Session ID must be an integer but was "
           << sessionTypeStatus.value();
        return Status::SemanticError(ss.str());
    }
    auto epIdStatus = deduceExprType(epIdExpr);
    if (!epIdStatus.ok()) {
        return epIdStatus.status();
    }
    if (epIdStatus.value() != Value::Type::INT) {
        std::stringstream ss;
        ss << epIdExpr->toString() << ", Session ID must be an integer but was "
           << epIdStatus.value();
        return Status::SemanticError(ss.str());
    }

    return Status::OK();
}

Status KillQueryValidator::toPlan() {
    auto sentence = static_cast<KillQuerySentence*>(sentence_);
    auto *node = KillQuery::make(qctx_, nullptr, sentence->sessionId(), sentence->epId());
    root_ = node;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
