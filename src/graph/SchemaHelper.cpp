/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "utils/ConvertTimeType.h"
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

StatusOr<nebula::cpp2::Value> SchemaHelper::toDefaultValue(ColumnSpecification* spec) {
    nebula::cpp2::Value v;
    Getters getter;
    CHECK(spec->hasDefaultValue());
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
            return v;
        }
        case nebula::ColumnType::INT: {
            if (value.which() != VAR_INT64) {
                LOG(ERROR) << "ValueType is wrong, input type "
                            << static_cast<int32_t>(type)
                            << ", value type " <<  value.which();
                return Status::Error("Wrong type");
            }
            v.set_int_value(boost::get<int64_t>(value));
            return v;
        }
        case nebula::ColumnType::DOUBLE: {
            if (value.which() != VAR_DOUBLE) {
                LOG(ERROR) << "ValueType is wrong, input type "
                            << static_cast<int32_t>(type)
                            << ", value type " <<  value.which();
                return Status::Error("Wrong type");
            }
            v.set_double_value(boost::get<double>(value));
            return v;
        }
        case nebula::ColumnType::STRING: {
            if (value.which() != VAR_STR) {
                LOG(ERROR) << "ValueType is wrong, input type "
                            << static_cast<int32_t>(type)
                            << ", value type " <<  value.which();
                return Status::Error("Wrong type");
            }
            v.set_string_value(boost::get<std::string>(value));
            return v;
        }
        case nebula::ColumnType::TIMESTAMP: {
            if (value.which() != VAR_INT64 && value.which() != VAR_STR) {
                LOG(ERROR) << "ValueType is wrong, input type "
                            << static_cast<int32_t>(type)
                            << ", value type " <<  value.which();
                return Status::Error("Wrong type");
            }
            auto timestamp = ConvertTimeType::toTimestamp(value);
            if (!timestamp.ok()) {
                return timestamp.status();
            }
            v.set_timestamp(timestamp.value());
            return v;
        }
        default:
            LOG(ERROR) << "Unsupported Type";
            return Status::Error("Unsupported type");
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
        if (spec->hasDefaultValue()) {
            auto statusV = toDefaultValue(spec);
            if (!statusV.ok()) {
                return statusV.status();
            }
            column.set_default_value(std::move(statusV).value());
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
    if (ttlColName.empty()) {
        schema.schema_prop.set_ttl_col("");
        return Status::OK();
    }
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
    Getters g;
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
                if (spec->hasDefaultValue()) {
                    auto statusV = toDefaultValue(spec);
                    if (!statusV.ok()) {
                        return statusV.status();
                    }
                    column.set_default_value(std::move(statusV).value());
                }
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
}   // namespace graph
}   // namespace nebula
