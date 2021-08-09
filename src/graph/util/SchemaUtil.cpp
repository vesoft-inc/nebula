/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "util/SchemaUtil.h"
#include "context/QueryContext.h"
#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

// static
Status SchemaUtil::validateProps(const std::vector<SchemaPropItem*> &schemaProps,
                                 meta::cpp2::Schema &schema) {
    auto status = Status::OK();
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
                case SchemaPropItem::COMMENT:
                    status = setComment(schemaProp, schema);
                    NG_RETURN_IF_ERROR(status);
                    break;
            }
        }

        auto &prop = *schema.schema_prop_ref();
        if (prop.get_ttl_duration() &&
            (*prop.get_ttl_duration() != 0)) {
            // Disable implicit TTL mode
            if (!prop.get_ttl_col() ||
                (prop.get_ttl_col() && prop.get_ttl_col()->empty())) {
                return Status::Error("Implicit ttl_col not support");
            }
        }
    }

    return Status::OK();
}

// static
std::shared_ptr<const meta::NebulaSchemaProvider> SchemaUtil::generateSchemaProvider(
    ObjectPool *pool,
    const SchemaVer ver,
    const meta::cpp2::Schema &schema) {
    auto schemaPtr = std::make_shared<meta::NebulaSchemaProvider>(ver);
    for (auto col : schema.get_columns()) {
        bool hasDef = col.default_value_ref().has_value();
        Expression* defaultValueExpr = nullptr;
        if (hasDef) {
            defaultValueExpr = Expression::decode(pool, *col.default_value_ref());
        }
        schemaPtr->addField(col.get_name(),
                            col.get_type().get_type(),
                            col.type.type_length_ref().value_or(0),
                            col.nullable_ref().value_or(false),
                            hasDef ? defaultValueExpr : nullptr);
    }
    return schemaPtr;
}

// static
Status SchemaUtil::setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema) {
    auto ret = schemaProp->getTtlDuration();
    if (!ret.ok()) {
        return ret.status();
    }

    auto ttlDuration = ret.value();
    schema.schema_prop_ref().value().set_ttl_duration(ttlDuration);
    return Status::OK();
}


// static
Status SchemaUtil::setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema) {
    auto ret = schemaProp->getTtlCol();
    if (!ret.ok()) {
        return ret.status();
    }

    auto  ttlColName = ret.value();
    if (ttlColName.empty()) {
        schema.schema_prop_ref().value().set_ttl_col("");
        return Status::OK();
    }
    // Check the legality of the ttl column name
    for (auto& col : *schema.columns_ref()) {
        if (col.name == ttlColName) {
            // Only integer columns and timestamp columns can be used as ttl_col
            // TODO(YT) Ttl_duration supports datetime type
            if (col.type.type != meta::cpp2::PropertyType::INT64 &&
                col.type.type != meta::cpp2::PropertyType::TIMESTAMP) {
                return Status::Error("Ttl column type illegal");
            }
            schema.schema_prop_ref().value().set_ttl_col(ttlColName);
            return Status::OK();
        }
    }
    return Status::Error("Ttl column name not exist in columns");
}

// static
Status SchemaUtil::setComment(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema) {
    auto ret = schemaProp->getComment();
    if (ret.ok()) {
        schema.schema_prop_ref()->set_comment(std::move(ret).value());
    }
    return Status::OK();
}

// static
StatusOr<Value> SchemaUtil::toVertexID(Expression *expr, Value::Type vidType) {
    QueryExpressionContext ctx;
    auto vidVal = expr->eval(ctx(nullptr));
    if (vidVal.type() != vidType) {
        LOG(ERROR) << expr->toString() << " is the wrong vertex id type: " << vidVal.typeName();
        return Status::Error("Wrong vertex id type: %s", expr->toString().c_str());
    }
    return vidVal;
}

// static
StatusOr<std::vector<Value>>
SchemaUtil::toValueVec(std::vector<Expression*> exprs) {
    std::vector<Value> values;
    values.reserve(exprs.size());
    QueryExpressionContext ctx;
    for (auto *expr : exprs) {
        auto value = expr->eval(ctx(nullptr));
         if (value.isNull() && value.getNull() != NullType::__NULL__) {
            LOG(ERROR) <<  expr->toString() << " is the wrong value type: " << value.typeName();
            return Status::Error("Wrong value type: %s", expr->toString().c_str());
        }
        values.emplace_back(std::move(value));
    }
    return values;
}

StatusOr<DataSet> SchemaUtil::toDescSchema(const meta::cpp2::Schema &schema) {
    DataSet dataSet({"Field", "Type", "Null", "Default", "Comment"});
    for (auto &col : schema.get_columns()) {
        Row row;
        row.values.emplace_back(Value(col.get_name()));
        row.values.emplace_back(typeToString(col));
        auto nullable = col.nullable_ref().has_value() ? *col.nullable_ref() : false;
        row.values.emplace_back(nullable ? "YES" : "NO");
        auto defaultValue = Value::kEmpty;
        ObjectPool tempPool;

        if (col.default_value_ref().has_value()) {
            auto expr = Expression::decode(&tempPool, *col.default_value_ref());
            if (expr == nullptr) {
                LOG(ERROR) << "Internal error: Wrong default value expression.";
                defaultValue = Value();
                continue;
            }
            if (expr->kind() == Expression::Kind::kConstant) {
                QueryExpressionContext ctx;
                defaultValue = Expression::eval(expr, ctx(nullptr));
            } else {
                defaultValue = Value(expr->toString());
            }
        }
        row.values.emplace_back(std::move(defaultValue));
        if (col.comment_ref().has_value()) {
            row.values.emplace_back(*col.comment_ref());
        } else {
            row.values.emplace_back();
        }
        dataSet.emplace_back(std::move(row));
    }
    return dataSet;
}

StatusOr<DataSet> SchemaUtil::toShowCreateSchema(bool isTag,
                                                 const std::string &name,
                                                 const meta::cpp2::Schema &schema) {
    DataSet dataSet;
    std::string createStr;
    createStr.reserve(1024);
    if (isTag) {
        dataSet.colNames = {"Tag", "Create Tag"};
        createStr = "CREATE TAG `" + name + "` (\n";
    } else {
        dataSet.colNames = {"Edge", "Create Edge"};
        createStr = "CREATE EDGE `" + name + "` (\n";
    }

    Row row;
    row.emplace_back(name);
    ObjectPool tempPool;
    for (auto &col : schema.get_columns()) {
        createStr += " `" + col.get_name() + "`";
        createStr += " " + typeToString(col);
        auto nullable = col.nullable_ref().has_value() ? *col.nullable_ref() : false;
        if (!nullable) {
            createStr += " NOT NULL";
        } else {
            createStr += " NULL";
        }

        if (col.default_value_ref().has_value()) {
            auto encodeStr = *col.default_value_ref();
            auto expr = Expression::decode(&tempPool, encodeStr);
            if (expr == nullptr) {
                LOG(ERROR) << "Internal error: the default value is wrong expression.";
                continue;
            }
            createStr += " DEFAULT " + expr->toString();
        }
        if (col.comment_ref().has_value()) {
            createStr += " COMMENT \"";
            createStr += *col.comment_ref();
            createStr += "\"";
        }
        createStr += ",\n";
    }
    if (!(*schema.columns_ref()).empty()) {
        createStr.resize(createStr.size() -2);
        createStr += "\n";
    }
    createStr += ")";
    auto prop = schema.get_schema_prop();
    createStr += " ttl_duration = ";
    if (prop.ttl_duration_ref().has_value()) {
        createStr += folly::to<std::string>(*prop.ttl_duration_ref());
    } else {
        createStr += "0";
    }
    createStr += ", ttl_col = ";
    if (prop.ttl_col_ref().has_value() && !(*prop.ttl_col_ref()).empty()) {
        createStr += "\"" + *prop.ttl_col_ref() + "\"";
    } else {
        createStr += "\"\"";
    }
    if (prop.comment_ref().has_value()) {
        createStr += ", comment = \"";
        createStr += *prop.comment_ref();
        createStr += "\"";
    }
    row.emplace_back(std::move(createStr));
    dataSet.rows.emplace_back(std::move(row));
    return dataSet;
}

std::string SchemaUtil::typeToString(const meta::cpp2::ColumnTypeDef &col) {
    auto type = apache::thrift::util::enumNameSafe(col.get_type());
    if (col.get_type() == meta::cpp2::PropertyType::FIXED_STRING) {
        return folly::stringPrintf("%s(%d)", type.c_str(), *col.get_type_length());
    }
    return type;
}

std::string SchemaUtil::typeToString(const meta::cpp2::ColumnDef &col) {
    auto str = typeToString(col.get_type());
    std::transform(
        std::begin(str), std::end(str), std::begin(str), [](uint8_t c) { return std::tolower(c); });
    return str;
}

Value::Type SchemaUtil::propTypeToValueType(meta::cpp2::PropertyType propType) {
    switch (propType) {
        case meta::cpp2::PropertyType::BOOL:
            return Value::Type::BOOL;
        case meta::cpp2::PropertyType::INT8:
        case meta::cpp2::PropertyType::INT16:
        case meta::cpp2::PropertyType::INT32:
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP:
            return Value::Type::INT;
        case meta::cpp2::PropertyType::TIME:
            return Value::Type::TIME;
        case meta::cpp2::PropertyType::VID:
            return Value::Type::STRING;
        case meta::cpp2::PropertyType::FLOAT:
        case meta::cpp2::PropertyType::DOUBLE:
            return Value::Type::FLOAT;
        case meta::cpp2::PropertyType::STRING:
        case meta::cpp2::PropertyType::FIXED_STRING:
            return Value::Type::STRING;
        case meta::cpp2::PropertyType::DATE:
            return Value::Type::DATE;
        case meta::cpp2::PropertyType::DATETIME:
            return Value::Type::DATETIME;
        case meta::cpp2::PropertyType::UNKNOWN:
            return Value::Type::__EMPTY__;
    }
    return Value::Type::__EMPTY__;
}

bool SchemaUtil::isValidVid(const Value &value, const meta::cpp2::ColumnTypeDef &type) {
    return isValidVid(value, type.get_type());
}

bool SchemaUtil::isValidVid(const Value &value, meta::cpp2::PropertyType type) {
    return isValidVid(value) && value.type() == propTypeToValueType(type);
}

bool SchemaUtil::isValidVid(const Value &value) {
    // compatible with 1.0
    return value.isStr() || value.isInt();
}

StatusOr<std::unique_ptr<std::vector<storage::cpp2::VertexProp>>>
SchemaUtil::getAllVertexProp(QueryContext *qctx, const SpaceInfo &space, bool withProp) {
    // Get all tags in the space
    const auto allTagsResult = qctx->schemaMng()->getAllLatestVerTagSchema(space.id);
    NG_RETURN_IF_ERROR(allTagsResult);
    // allTags: std::unordered_map<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>
    const auto allTags = std::move(allTagsResult).value();

    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    vertexProps->reserve(allTags.size());
    // Retrieve prop names of each tag and append "_tag" to the name list to query empty tags
    for (const auto &tag : allTags) {
        // tag: pair<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>
        std::vector<std::string> propNames;
        if (withProp) {
            const auto tagSchema = tag.second;   // nebulaSchemaProvider
            for (size_t i = 0; i < tagSchema->getNumFields(); i++) {
                const auto propName = tagSchema->getFieldName(i);
                propNames.emplace_back(propName);
            }
        }
        storage::cpp2::VertexProp vProp;
        const auto tagId = tag.first;
        vProp.set_tag(tagId);
        propNames.emplace_back(nebula::kTag);   // "_tag"
        vProp.set_props(std::move(propNames));
        vertexProps->emplace_back(std::move(vProp));
    }
    return vertexProps;
}

StatusOr<std::unique_ptr<std::vector<storage::cpp2::EdgeProp>>> SchemaUtil::getEdgeProps(
    QueryContext *qctx,
    const SpaceInfo &space,
    const std::vector<EdgeType> &edgeTypes,
    bool withProp) {
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    edgeProps->reserve(edgeTypes.size());
    for (const auto &edgeType : edgeTypes) {
        std::vector<std::string> propNames = {kSrc, kType, kRank, kDst};
        if (withProp) {
            auto edgeSchema = qctx->schemaMng()->getEdgeSchema(space.id, std::abs(edgeType));
            for (size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
                propNames.emplace_back(edgeSchema->getFieldName(i));
            }
        }
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props(std::move(propNames));
        edgeProps->emplace_back(std::move(prop));
    }
    return edgeProps;
}
}  // namespace graph
}  // namespace nebula
