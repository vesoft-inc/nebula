/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Map.h"
#include "common/conf/Configuration.h"
#include "common/expression/ConstantExpression.h"
#include "common/interface/gen-cpp2/meta_types.h"

#include "kvstore/Common.h"
#include "meta/MetaServiceUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/upgradeData/MetaDataUpgrade.h"
#include "meta/upgradeData/oldThrift/MetaServiceUtilsV1.h"

DECLARE_bool(null_type);
DECLARE_uint32(string_index_limit);

namespace nebula {
namespace meta {

Status MetaDataUpgrade::rewriteHosts(const folly::StringPiece &key,
                                     const folly::StringPiece &val) {
    auto host = oldmeta::MetaServiceUtilsV1::parseHostKey(key);
    auto info = HostInfo::decodeV1(val);
    auto newVal = HostInfo::encodeV2(info);
    auto newKey = MetaServiceUtils::hostKeyV2(
            network::NetworkUtils::intToIPv4(host.get_ip()), host.get_port());
    NG_LOG_AND_RETURN_IF_ERROR(put(newKey, newVal));
    NG_LOG_AND_RETURN_IF_ERROR(remove(key));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteLeaders(const folly::StringPiece &key,
                                       const folly::StringPiece &val) {
    auto host = oldmeta::MetaServiceUtilsV1::parseLeaderKey(key);
    auto newKey = MetaServiceUtils::leaderKey(
            network::NetworkUtils::intToIPv4(host.get_ip()), host.get_port());
    NG_LOG_AND_RETURN_IF_ERROR(put(newKey, val));
    NG_LOG_AND_RETURN_IF_ERROR(remove(key));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteSpaces(const folly::StringPiece &key,
                                      const folly::StringPiece &val) {
    auto oldProps = oldmeta::MetaServiceUtilsV1::parseSpace(val);
    cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name(oldProps.get_space_name());
    spaceDesc.set_partition_num(oldProps.get_partition_num());
    spaceDesc.set_replica_factor(oldProps.get_replica_factor());
    spaceDesc.set_charset_name(oldProps.get_charset_name());
    spaceDesc.set_collate_name(oldProps.get_collate_name());
    (*spaceDesc.vid_type_ref()).set_type_length(8);
    (*spaceDesc.vid_type_ref()).set_type(cpp2::PropertyType::INT64);
    NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaServiceUtils::spaceVal(spaceDesc)));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteParts(const folly::StringPiece &key,
                                    const folly::StringPiece &val) {
    auto oldHosts = oldmeta::MetaServiceUtilsV1::parsePartVal(val);
    std::vector<HostAddr> newHosts;
    for (auto &host : oldHosts) {
        HostAddr hostAddr;
        hostAddr.host = network::NetworkUtils::intToIPv4(host.get_ip());
        hostAddr.port = host.get_port();
        newHosts.emplace_back(std::move(hostAddr));
    }
    NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaServiceUtils::partVal(newHosts)));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteSchemas(const folly::StringPiece &key,
                                       const folly::StringPiece &val) {
    auto oldSchema = oldmeta::MetaServiceUtilsV1::parseSchema(val);
    cpp2::Schema newSchema;
    cpp2::SchemaProp newSchemaProps;
    auto &schemaProp = oldSchema.get_schema_prop();
    if (schemaProp.ttl_duration_ref().has_value()) {
        newSchemaProps.set_ttl_duration(*schemaProp.get_ttl_duration());
    }
    if (schemaProp.ttl_col_ref().has_value()) {
        newSchemaProps.set_ttl_col(*schemaProp.get_ttl_col());
    }
    newSchema.set_schema_prop(std::move(newSchemaProps));
    NG_LOG_AND_RETURN_IF_ERROR(convertToNewColumns((*oldSchema.columns_ref()),
                (*newSchema.columns_ref())));

    auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto schemaName = val.subpiece(sizeof(int32_t), nameLen).str();
    auto encodeVal = MetaServiceUtils::schemaVal(schemaName, newSchema);
    NG_LOG_AND_RETURN_IF_ERROR(put(key, encodeVal));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteIndexes(const folly::StringPiece &key,
                                       const folly::StringPiece &val) {
    auto oldItem = oldmeta::MetaServiceUtilsV1::parseIndex(val);
    cpp2::IndexItem newItem;
    newItem.set_index_id(oldItem.get_index_id());
    newItem.set_index_name(oldItem.get_index_name());
    cpp2::SchemaID schemaId;
    if (oldItem.get_schema_id().getType() == oldmeta::cpp2::SchemaID::Type::tag_id) {
        schemaId.set_tag_id(oldItem.get_schema_id().get_tag_id());
    } else {
        schemaId.set_edge_type(oldItem.get_schema_id().get_edge_type());
    }
    newItem.set_schema_id(schemaId);
    NG_LOG_AND_RETURN_IF_ERROR(convertToNewIndexColumns((*oldItem.fields_ref()),
                (*newItem.fields_ref())));
    NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaServiceUtils::indexVal(newItem)));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteConfigs(const folly::StringPiece &key,
                                       const folly::StringPiece &val) {
    auto item = oldmeta::MetaServiceUtilsV1::parseConfigValue(val);

    Value configVal;
    switch (item.get_type()) {
        case oldmeta::cpp2::ConfigType::INT64: {
            auto value = *reinterpret_cast<const int64_t *>(item.get_value().data());
            configVal.setInt(boost::get<int64_t>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::DOUBLE: {
            auto value = *reinterpret_cast<const double *>(item.get_value().data());
            configVal.setFloat(boost::get<double>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::BOOL: {
            auto value = *reinterpret_cast<const bool *>(item.get_value().data());
            configVal.setBool(boost::get<bool>(value) ? "True" : "False");
            break;
        }
        case oldmeta::cpp2::ConfigType::STRING: {
            configVal.setStr(boost::get<std::string>(item.get_value()));
            break;
        }
        case oldmeta::cpp2::ConfigType::NESTED: {
            auto value = item.get_value();
            // transform to map value
            conf::Configuration conf;
            auto status = conf.parseFromString(boost::get<std::string>(value));
            if (!status.ok()) {
                LOG(ERROR) << "Parse value: " << value
                           << " failed: " << status;
                return Status::Error("Parse value: %s failed", value.c_str());
            }
            Map map;
            conf.forEachItem(
                    [&map](const folly::StringPiece&confKey, const folly::dynamic &confVal) {
                map.kvs.emplace(confKey, confVal.asString());
            });
            configVal.setMap(std::move(map));
            break;
        }
    }
    auto newVal = MetaServiceUtils::configValue(
            static_cast<cpp2::ConfigMode>(item.get_mode()), configVal);
    NG_LOG_AND_RETURN_IF_ERROR(put(key, newVal));
    return Status::OK();
}

Status MetaDataUpgrade::rewriteJobDesc(const folly::StringPiece &key,
                                       const folly::StringPiece &val) {
    auto jobDesc = oldmeta::MetaServiceUtilsV1::parseJobDesc(val);
    auto cmdStr = std::get<0>(jobDesc);
    nebula::meta::cpp2::AdminCmd adminCmd;
    if (cmdStr.find("flush") == 0) {
        adminCmd = nebula::meta::cpp2::AdminCmd::FLUSH;
    } else if (cmdStr.find("compact") == 0) {
        adminCmd = nebula::meta::cpp2::AdminCmd::COMPACT;
    } else {
        return Status::Error("Wrong job cmd: %s", cmdStr.c_str());
    }
    auto paras = std::get<1>(jobDesc);
    auto status = std::get<2>(jobDesc);
    auto startTime = std::get<3>(jobDesc);
    auto stopTime = std::get<4>(jobDesc);
    std::string str;
    str.reserve(256);
    // use a big num to avoid possible conflict
    int32_t dataVersion = INT_MAX - 1;
    str.append(reinterpret_cast<const char*>(&dataVersion), sizeof(dataVersion))
            .append(reinterpret_cast<const char*>(&adminCmd), sizeof(adminCmd));
    auto paraSize = paras.size();
    str.append(reinterpret_cast<const char*>(&paraSize), sizeof(size_t));
    for (auto& para : paras) {
        auto len = para.length();
        str.append(reinterpret_cast<const char*>(&len), sizeof(len))
                .append(reinterpret_cast<const char*>(&para[0]), len);
    }
    str.append(reinterpret_cast<const char*>(&status), sizeof(nebula::meta::cpp2::JobStatus))
            .append(reinterpret_cast<const char*>(&startTime), sizeof(int64_t))
            .append(reinterpret_cast<const char*>(&stopTime), sizeof(int64_t));

    NG_LOG_AND_RETURN_IF_ERROR(put(key, str));
    return Status::OK();
}

Status MetaDataUpgrade::deleteKeyVal(const folly::StringPiece &key) {
    NG_LOG_AND_RETURN_IF_ERROR(remove(key));
    return Status::OK();
}

Status MetaDataUpgrade::convertToNewColumns(const std::vector<oldmeta::cpp2::ColumnDef> &oldCols,
                                            std::vector<cpp2::ColumnDef> &newCols) {
    ObjectPool objPool;
    auto pool = &objPool;
    for (auto &colDef : oldCols) {
        cpp2::ColumnDef columnDef;
        columnDef.set_name(colDef.get_name());
        columnDef.type.set_type(static_cast<cpp2::PropertyType>(colDef.get_type().get_type()));
        if (colDef.default_value_ref().has_value()) {
            std::string encodeStr;
            switch (colDef.get_type().get_type()) {
                case oldmeta::cpp2::SupportedType::BOOL:
                    encodeStr = Expression::encode(*ConstantExpression::make(
                        pool, colDef.get_default_value()->get_bool_value()));
                    break;
                case oldmeta::cpp2::SupportedType::INT:
                    encodeStr = Expression::encode(*ConstantExpression::make(
                        pool, colDef.get_default_value()->get_int_value()));
                    break;
                case oldmeta::cpp2::SupportedType::DOUBLE:
                    encodeStr = Expression::encode(*ConstantExpression::make(
                        pool, colDef.get_default_value()->get_double_value()));
                    break;
                case oldmeta::cpp2::SupportedType::STRING:
                    encodeStr = Expression::encode(*ConstantExpression::make(
                        pool, colDef.get_default_value()->get_string_value()));
                    break;
                case oldmeta::cpp2::SupportedType::TIMESTAMP:
                    encodeStr = Expression::encode(*ConstantExpression::make(
                        pool, colDef.get_default_value()->get_timestamp()));
                    break;
                default:
                    return Status::Error(
                        "Wrong default type: %s",
                        apache::thrift::util::enumNameSafe(colDef.get_type().get_type()).c_str());
            }

            columnDef.set_default_value(std::move(encodeStr));
        }
        if (FLAGS_null_type) {
            columnDef.set_nullable(true);
        }
        newCols.emplace_back(std::move(columnDef));
    }
    return Status::OK();
}

Status
MetaDataUpgrade::convertToNewIndexColumns(const std::vector<oldmeta::cpp2::ColumnDef> &oldCols,
                                          std::vector<cpp2::ColumnDef> &newCols) {
    for (auto &colDef : oldCols) {
        cpp2::ColumnDef columnDef;
        columnDef.set_name(colDef.get_name());
        if (colDef.get_type().get_type() == oldmeta::cpp2::SupportedType::STRING) {
            cpp2::ColumnTypeDef type;
            type.set_type(cpp2::PropertyType::FIXED_STRING);
            type.set_type_length(FLAGS_string_index_limit);
            columnDef.set_type(std::move(type));
        } else {
            columnDef.type.set_type(static_cast<cpp2::PropertyType>(colDef.get_type().get_type()));
        }
        DCHECK(!colDef.default_value_ref().has_value());
        if (FLAGS_null_type) {
            columnDef.set_nullable(true);
        }
        newCols.emplace_back(std::move(columnDef));
    }
    return Status::OK();
}

void MetaDataUpgrade::printHost(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto host = oldmeta::MetaServiceUtilsV1::parseHostKey(key);
    auto info = HostInfo::decodeV1(val);
    LOG(INFO) << "Host ip: " << network::NetworkUtils::intToIPv4(host.get_ip());
    LOG(INFO) << "Host port: " << host.get_port();
    LOG(INFO) << "Host info: lastHBTimeInMilliSec: " << info.lastHBTimeInMilliSec_;
    LOG(INFO) << "Host info: role_: " << apache::thrift::util::enumNameSafe(info.role_);
    LOG(INFO) << "Host info: gitInfoSha_: " << info.gitInfoSha_;
}

void MetaDataUpgrade::printSpaces(const folly::StringPiece &val) {
    auto oldProps = oldmeta::MetaServiceUtilsV1::parseSpace(val);
    LOG(INFO) << "Space name: " << oldProps.get_space_name();
    LOG(INFO) << "Partition num: " << oldProps.get_partition_num();
    LOG(INFO) << "Replica factor: " << oldProps.get_replica_factor();
    LOG(INFO) << "Charset name: " << oldProps.get_charset_name();
    LOG(INFO) << "Collate name: " << oldProps.get_collate_name();
}

void MetaDataUpgrade::printParts(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto spaceId = oldmeta::MetaServiceUtilsV1::parsePartKeySpaceId(key);
    auto partId = oldmeta::MetaServiceUtilsV1::parsePartKeyPartId(key);
    auto oldHosts = oldmeta::MetaServiceUtilsV1::parsePartVal(val);
    LOG(INFO) << "Part spaceId: " << spaceId;
    LOG(INFO) << "Part      id: " << partId;
    for (auto &host : oldHosts) {
        LOG(INFO) << "Part host   ip: " << network::NetworkUtils::intToIPv4(host.get_ip());
        LOG(INFO) << "Part host port: " << host.get_port();
    }
}

void MetaDataUpgrade::printLeaders(const folly::StringPiece &key) {
    auto host = oldmeta::MetaServiceUtilsV1::parseLeaderKey(key);
    LOG(INFO) << "Leader host ip: " << network::NetworkUtils::intToIPv4(host.get_ip());
    LOG(INFO) << "Leader host port: " << host.get_port();
}

void MetaDataUpgrade::printSchemas(const folly::StringPiece &val) {
    auto oldSchema = oldmeta::MetaServiceUtilsV1::parseSchema(val);
    auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto schemaName = val.subpiece(sizeof(int32_t), nameLen).str();
    LOG(INFO) << "Schema name: " << schemaName;
    for (auto &colDef : oldSchema.get_columns()) {
        LOG(INFO) << "Schema column name: " << colDef.get_name();
        LOG(INFO) << "Schema column type: "
                  << apache::thrift::util::enumNameSafe(
                          colDef.get_type().get_type());
        Value defaultValue;
        if (colDef.default_value_ref().has_value()) {
            switch (colDef.get_type().get_type()) {
                case oldmeta::cpp2::SupportedType::BOOL:
                    defaultValue = colDef.get_default_value()->get_bool_value();
                    break;
                case oldmeta::cpp2::SupportedType::INT:
                    defaultValue = colDef.get_default_value()->get_int_value();
                    break;
                case oldmeta::cpp2::SupportedType::DOUBLE:
                    defaultValue = colDef.get_default_value()->get_double_value();
                    break;
                case oldmeta::cpp2::SupportedType::STRING:
                    defaultValue = colDef.get_default_value()->get_string_value();
                    break;
                case oldmeta::cpp2::SupportedType::TIMESTAMP:
                    defaultValue = colDef.get_default_value()->get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Wrong default type: "
                               << apache::thrift::util::enumNameSafe(
                                       colDef.get_type().get_type());
            }
            LOG(INFO) << "Schema default value: " << defaultValue;
        }
    }
}

void MetaDataUpgrade::printIndexes(const folly::StringPiece &val) {
    auto oldItem = oldmeta::MetaServiceUtilsV1::parseIndex(val);
    LOG(INFO) << "Index   id: " << oldItem.get_index_id();
    LOG(INFO) << "Index name: " << oldItem.get_index_name();
    if (oldItem.get_schema_id().getType() == oldmeta::cpp2::SchemaID::Type::tag_id) {
        LOG(INFO) << "Index on tag id: " << oldItem.get_schema_id().get_tag_id();
    } else {
        LOG(INFO) << "Index on edgetype: " << oldItem.get_schema_id().get_edge_type();
    }
    for (auto &colDef : oldItem.get_fields()) {
        LOG(INFO) << "Index field name: " << colDef.get_name();
        LOG(INFO) << "Index field type: "
                  << apache::thrift::util::enumNameSafe(
                          colDef.get_type().get_type());
    }
}

void MetaDataUpgrade::printConfigs(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto item = oldmeta::MetaServiceUtilsV1::parseConfigValue(val);
    auto configName = oldmeta::MetaServiceUtilsV1::parseConfigKey(key);
    Value configVal;
    switch (item.get_type()) {
        case oldmeta::cpp2::ConfigType::INT64: {
            auto value = *reinterpret_cast<const int64_t *>(item.get_value().data());
            configVal.setInt(boost::get<int64_t>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::DOUBLE: {
            auto value = *reinterpret_cast<const double *>(item.get_value().data());
            configVal.setFloat(boost::get<double>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::BOOL: {
            auto value = *reinterpret_cast<const bool *>(item.get_value().data());
            configVal.setBool(boost::get<bool>(value) ? "True" : "False");
            break;
        }
        case oldmeta::cpp2::ConfigType::STRING: {
            configVal.setStr(boost::get<std::string>(item.get_value()));
            break;
        }
        case oldmeta::cpp2::ConfigType::NESTED: {
            auto value = item.get_value();
            // transform to map value
            conf::Configuration conf;
            auto status = conf.parseFromString(boost::get<std::string>(value));
            if (!status.ok()) {
                LOG(ERROR) << "Parse value: " << value
                           << " failed: " << status;
                return;
            }
            Map map;
            conf.forEachItem(
                    [&map](const folly::StringPiece &confKey, const folly::dynamic &confVal) {
                map.kvs.emplace(confKey, confVal.asString());
            });
            configVal.setMap(std::move(map));
            break;
        }
    }
    LOG(INFO) << "Config   name: " << configName.second;
    LOG(INFO) << "Config module: "
              << apache::thrift::util::enumNameSafe(configName.first);
    LOG(INFO) << "Config   mode: "
              << apache::thrift::util::enumNameSafe(item.get_mode());
    LOG(INFO) << "Config  value: " << configVal;
}

void MetaDataUpgrade::printJobDesc(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto jobId = oldmeta::MetaServiceUtilsV1::parseJobId(key);
    LOG(INFO) << "JobDesc id: " << jobId;
    auto jobDesc = oldmeta::MetaServiceUtilsV1::parseJobDesc(val);
    auto cmdStr = std::get<0>(jobDesc);
    auto paras = std::get<1>(jobDesc);
    auto status = std::get<2>(jobDesc);
    auto startTime = std::get<3>(jobDesc);
    auto stopTime = std::get<4>(jobDesc);

    LOG(INFO) << "JobDesc id: " << jobId;
    LOG(INFO) << "JobDesc cmd: " << cmdStr;
    for (auto &para : paras) {
        LOG(INFO) << "JobDesc para: " << para;
    }
    LOG(INFO) << "JobDesc status: " << apache::thrift::util::enumNameSafe(status);
    LOG(INFO) << "JobDesc startTime: " << startTime;
    LOG(INFO) << "JobDesc stopTime: " << stopTime;
}

}  // namespace meta
}  // namespace nebula

