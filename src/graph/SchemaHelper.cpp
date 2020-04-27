/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/SchemaHelper.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

// static
nebula::cpp2::SupportedType SchemaHelper::columnTypeToSupportedType(nebula::ColumnType type) {
    switch (type) {
        case nebula::ColumnType::BOOL:
            return nebula::cpp2::SupportedType::BOOL;
        case nebula::ColumnType::INT:
            return nebula::cpp2::SupportedType::INT;
        case nebula::ColumnType::DOUBLE:
            return nebula::cpp2::SupportedType::DOUBLE;
        case nebula::ColumnType::STRING:
            return nebula::cpp2::SupportedType::STRING;
        case nebula::ColumnType::TIMESTAMP:
            return nebula::cpp2::SupportedType::TIMESTAMP;
        default:
            return nebula::cpp2::SupportedType::UNKNOWN;
    }
}

// static
Status SchemaHelper::createSchema(const std::vector<ColumnSpecification*>& specs,
                                  const std::vector<SchemaPropItem*>& schemaProps,
                                  nebula::cpp2::Schema& schema) {
    auto status = Status::OK();

    std::unordered_set<std::string> nameSet;
    for (auto& spec : specs) {
        if (nameSet.find(*spec->name()) != nameSet.end()) {
            return Status::Error("Duplicate column name `%s'", spec->name()->c_str());
        }
        nameSet.emplace(*spec->name());
        nebula::cpp2::ColumnDef column;
        column.name = *spec->name();
        column.type.type = columnTypeToSupportedType(spec->type());
        nebula::cpp2::Value v;
        Getters getter;
        if (spec->hasDefault()) {
            auto s = spec->prepare();
            if (!s.ok()) {
                return s;
            }
            auto valStatus = spec->getDefault(getter);
            if (!valStatus.ok()) {
                return std::move(valStatus).status();
            }
            auto value = std::move(valStatus).value();
            auto type = spec->type();
            switch (type) {
                case nebula::ColumnType::BOOL: {
                    if (value.which() != VAR_BOOL) {
                        LOG(ERROR) << "ValueType is wrong, input type "
                                   << static_cast<int32_t>(type)
                                   << ", value type " <<  value.which();
                        return Status::Error("Wrong type");
                    }
                    v.set_bool_value(boost::get<bool>(value));
                    break;
                }
                case nebula::ColumnType::INT: {
                    if (value.which() != VAR_INT64) {
                        LOG(ERROR) << "ValueType is wrong, input type "
                                   << static_cast<int32_t>(type)
                                   << ", value type " <<  value.which();
                        return Status::Error("Wrong type");
                    }
                    v.set_int_value(boost::get<int64_t>(value));
                    break;
                }
                case nebula::ColumnType::DOUBLE: {
                    if (value.which() != VAR_DOUBLE) {
                        LOG(ERROR) << "ValueType is wrong, input type "
                                   << static_cast<int32_t>(type)
                                   << ", value type " <<  value.which();
                        return Status::Error("Wrong type");
                    }
                    v.set_double_value(boost::get<double>(value));
                    break;
                }
                case nebula::ColumnType::STRING: {
                    if (value.which() != VAR_STR) {
                        LOG(ERROR) << "ValueType is wrong, input type "
                                   << static_cast<int32_t>(type)
                                   << ", value type " <<  value.which();
                        return Status::Error("Wrong type");
                    }
                    v.set_string_value(boost::get<std::string>(value));
                    break;
                }
                case nebula::ColumnType::TIMESTAMP: {
                    if (value.which() != VAR_INT64 && value.which() != VAR_STR) {
                        LOG(ERROR) << "ValueType is wrong, input type "
                                   << static_cast<int32_t>(type)
                                   << ", value type " <<  value.which();
                        return Status::Error("Wrong type");
                    }
                    auto timestamp = toTimestamp(value);
                    if (!timestamp.ok()) {
                        return timestamp.status();
                    }
                    v.set_timestamp(timestamp.value());
                    break;
                }
                default:
                    LOG(ERROR) << "Unsupported Type";
                    return Status::Error("Unsupported type");
            }
            column.set_default_value(std::move(v));
        }
        schema.columns.emplace_back(std::move(column));
    }

    if (!schemaProps.empty()) {
        for (auto& schemaProp : schemaProps) {
            switch (schemaProp->getPropType()) {
                case SchemaPropItem::TTL_DURATION:
                    status = setTTLDuration(schemaProp, schema);
                    if (!status.ok()) {
                        return status;
                    }
                    break;
                case SchemaPropItem::TTL_COL:
                    status = setTTLCol(schemaProp, schema);
                    if (!status.ok()) {
                        return status;
                    }
                    break;
            }
        }

        if (schema.schema_prop.get_ttl_duration() &&
            (*schema.schema_prop.get_ttl_duration() != 0)) {
            // Disable implicit TTL mode
            if (!schema.schema_prop.get_ttl_col() ||
                (schema.schema_prop.get_ttl_col() && schema.schema_prop.get_ttl_col()->empty())) {
                return Status::Error("Implicit ttl_col not support");
            }
        }
    }

    return Status::OK();
}


// static
Status SchemaHelper::setTTLDuration(SchemaPropItem* schemaProp, nebula::cpp2::Schema& schema) {
    auto ret = schemaProp->getTtlDuration();
    if (!ret.ok()) {
        return ret.status();
    }

    auto ttlDuration = ret.value();
    schema.schema_prop.set_ttl_duration(ttlDuration);
    return Status::OK();
}


// static
Status SchemaHelper::setTTLCol(SchemaPropItem* schemaProp, nebula::cpp2::Schema& schema) {
    auto ret = schemaProp->getTtlCol();
    if (!ret.ok()) {
        return ret.status();
    }

    auto  ttlColName = ret.value();
    // Check the legality of the ttl column name
    for (auto& col : schema.columns) {
        if (col.name == ttlColName) {
            // Only integer columns and timestamp columns can be used as ttl_col
            // TODO(YT) Ttl_duration supports datetime type
            if (col.type.type != nebula::cpp2::SupportedType::INT &&
                col.type.type != nebula::cpp2::SupportedType::TIMESTAMP) {
                return Status::Error("Ttl column type illegal");
            }
            schema.schema_prop.set_ttl_col(ttlColName);
            return Status::OK();
        }
    }
    return Status::Error("Ttl column name not exist in columns");
}


// static
Status SchemaHelper::alterSchema(const std::vector<AlterSchemaOptItem*>& schemaOpts,
                                 const std::vector<SchemaPropItem*>& schemaProps,
                                 std::vector<nebula::meta::cpp2::AlterSchemaItem>& options,
                                 nebula::cpp2::SchemaProp& prop) {
    for (auto& schemaOpt : schemaOpts) {
        nebula::meta::cpp2::AlterSchemaItem schemaItem;
        auto opType = schemaOpt->toType();
        schemaItem.set_op(opType);
        nebula::cpp2::Schema schema;
        if (opType == nebula::meta::cpp2::AlterSchemaOp::DROP) {
            const auto& colNames = schemaOpt->columnNames();
            for (auto& colName : colNames) {
                nebula::cpp2::ColumnDef column;
                column.name = *colName;
                schema.columns.emplace_back(std::move(column));
            }
        } else {
            const auto& specs = schemaOpt->columnSpecs();
            for (auto& spec : specs) {
                nebula::cpp2::ColumnDef column;
                column.name = *spec->name();
                column.type.type = columnTypeToSupportedType(spec->type());
                schema.columns.emplace_back(std::move(column));
            }
        }

        schemaItem.set_schema(std::move(schema));
        options.emplace_back(std::move(schemaItem));
    }

    for (auto& schemaProp : schemaProps) {
        auto propType = schemaProp->getPropType();
        StatusOr<int64_t> retInt;
        StatusOr<std::string> retStr;
        int ttlDuration;
        switch (propType) {
            case SchemaPropItem::TTL_DURATION:
                retInt = schemaProp->getTtlDuration();
                if (!retInt.ok()) {
                   return retInt.status();
                }
                ttlDuration = retInt.value();
                prop.set_ttl_duration(ttlDuration);
                break;
            case SchemaPropItem::TTL_COL:
                // Check the legality of the column in meta
                retStr = schemaProp->getTtlCol();
                if (!retStr.ok()) {
                   return retStr.status();
                }
                prop.set_ttl_col(retStr.value());
                break;
            default:
                return Status::Error("Property type not support");
        }
    }
    return Status::OK();
}

StatusOr<int64_t> SchemaHelper::toTimestamp(const VariantType &value) {
    if (value.which() != VAR_INT64 && value.which() != VAR_STR) {
        return Status::Error("Invalid value type");
    }

    int64_t timestamp;
    if (value.which() == VAR_STR) {
        static const std::regex reg("^([1-9]\\d{3})-"
                                    "(0[1-9]|1[0-2]|\\d)-"
                                    "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+"
                                    "(20|21|22|23|[0-1]\\d|\\d):"
                                    "([0-5]\\d|\\d):"
                                    "([0-5]\\d|\\d)$");
        std::smatch result;
        if (!std::regex_match(boost::get<std::string>(value), result, reg)) {
            return Status::Error("Invalid timestamp type");
        }
        struct tm time;
        memset(&time, 0, sizeof(time));
        time.tm_year = atoi(result[1].str().c_str()) - 1900;
        time.tm_mon = atoi(result[2].str().c_str()) - 1;
        time.tm_mday = atoi(result[3].str().c_str());
        time.tm_hour = atoi(result[4].str().c_str());
        time.tm_min = atoi(result[5].str().c_str());
        time.tm_sec = atoi(result[6].str().c_str());
        timestamp = mktime(&time);
    } else {
        timestamp = boost::get<int64_t>(value);
    }

    // The mainstream Linux kernel's implementation constrains this
    static const int64_t maxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;
    if (timestamp < 0 || (timestamp > maxTimestamp)) {
        return Status::Error("Invalid timestamp type");
    }
    return timestamp;
}
}   // namespace graph
}   // namespace nebula
