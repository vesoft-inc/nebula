/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ToJson.h"

#include <folly/String.h>
#include <folly/dynamic.h>
#include <string>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/clients/meta/MetaClient.h"
#include "common/datatypes/Value.h"
#include "common/expression/Expression.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "context/Symbols.h"
#include "parser/EdgeKey.h"
#include "util/SchemaUtil.h"

#include "context/QueryExpressionContext.h"
#include "parser/EdgeKey.h"

namespace nebula {
namespace util {

std::string toJson(const std::string &str) {
    return str;
}

std::string toJson(int32_t i) {
    return folly::to<std::string>(i);
}

std::string toJson(int64_t i) {
    return folly::to<std::string>(i);
}

std::string toJson(size_t i) {
    return folly::to<std::string>(i);
}

std::string toJson(bool b) {
    return b ? "true" : "false";
}

std::string toJson(const HostAddr &addr) {
    return addr.toString();
}

std::string toJson(const List &list) {
    return list.toString();
}

std::string toJson(const Value &value) {
    return value.toString();
}

std::string toJson(const EdgeKeyRef *ref) {
    return ref->toString();
}

std::string toJson(const Expression *expr) {
    return expr->toString();
}

folly::dynamic toJson(const meta::cpp2::SpaceDesc &desc) {
    folly::dynamic obj = folly::dynamic::object();
    obj.insert("name", *desc.space_name_ref());
    obj.insert("partNum", *desc.partition_num_ref());
    obj.insert("replicaFactor", *desc.replica_factor_ref());
    obj.insert("charset", *desc.charset_name_ref());
    obj.insert("collate", *desc.collate_name_ref());
    obj.insert("vidType", graph::SchemaUtil::typeToString(desc.get_vid_type()));
    return obj;
}

folly::dynamic toJson(const meta::cpp2::ColumnDef &column) {
    folly::dynamic obj = folly::dynamic::object();
    obj.insert("name", column.get_name());
    obj.insert("type", apache::thrift::util::enumNameSafe(column.get_type().get_type()));
    if (column.type.type_length_ref().has_value()) {
        obj.insert("typeLength", folly::to<std::string>(*column.get_type().type_length_ref()));
    }
    if (column.nullable_ref().has_value()) {
        obj.insert("nullable", folly::to<std::string>(*column.nullable_ref()));
    }
    if (column.default_value_ref().has_value()) {
        graph::QueryExpressionContext ctx;
        ObjectPool tempPool;
        auto value = Expression::decode(&tempPool, *column.default_value_ref());
        obj.insert("defaultValue", value->toString());
    }
    return obj;
}

folly::dynamic toJson(const meta::cpp2::Schema &schema) {
    folly::dynamic json = folly::dynamic::object();
    if (schema.columns_ref().is_set()) {
        json.insert("columns", toJson(*schema.columns_ref()));
    }
    if (schema.schema_prop_ref().is_set()) {
        json.insert("prop", toJson(*schema.schema_prop_ref()));
    }
    return json;
}

folly::dynamic toJson(const meta::cpp2::SchemaProp &prop) {
    folly::dynamic object = folly::dynamic::object();
    if (prop.ttl_col_ref().has_value()) {
        object.insert("ttlCol", *prop.ttl_col_ref());
    }
    if (prop.ttl_duration_ref()) {
        object.insert("ttlDuration", *prop.ttl_duration_ref());
    }
    return object;
}

folly::dynamic toJson(const meta::cpp2::AlterSchemaItem &item) {
    folly::dynamic json = folly::dynamic::object();
    if (item.schema_ref().is_set()) {
        json.insert("schema", toJson(*item.schema_ref()));
    }
    if (item.op_ref().is_set()) {
        json.insert("op", apache::thrift::util::enumNameSafe(*item.op_ref()));
    }
    return json;
}

folly::dynamic toJson(const storage::cpp2::EdgeKey &edgeKey) {
    folly::dynamic edgeKeyObj = folly::dynamic::object();
    if (edgeKey.src_ref().is_set()) {
        edgeKeyObj.insert("src", toJson(*edgeKey.src_ref()));
    }
    if (edgeKey.dst_ref().is_set()) {
        edgeKeyObj.insert("dst", toJson(*edgeKey.dst_ref()));
    }
    if (edgeKey.edge_type_ref().is_set()) {
        edgeKeyObj.insert("edgeType", *edgeKey.edge_type_ref());
    }
    if (edgeKey.ranking_ref().is_set()) {
        edgeKeyObj.insert("ranking", *edgeKey.ranking_ref());
    }
    return edgeKeyObj;
}

folly::dynamic toJson(const storage::cpp2::NewTag &tag) {
    folly::dynamic tagObj = folly::dynamic::object();
    if (tag.tag_id_ref().is_set()) {
        tagObj.insert("tagId", *tag.tag_id_ref());
    }
    if (tag.props_ref().is_set()) {
        tagObj.insert("props", toJson(*tag.props_ref()));
    }
    return tagObj;
}

folly::dynamic toJson(const storage::cpp2::NewVertex &vert) {
    folly::dynamic vertObj = folly::dynamic::object();
    if (vert.id_ref().is_set()) {
        vertObj.insert("id", toJson(*vert.id_ref()));
    }
    if (vert.tags_ref().is_set()) {
        vertObj.insert("tags", util::toJson(*vert.tags_ref()));
    }
    return vertObj;
}

folly::dynamic toJson(const storage::cpp2::NewEdge &edge) {
    folly::dynamic edgeObj = folly::dynamic::object();
    if (edge.key_ref().is_set()) {
        edgeObj.insert("key", toJson(*edge.key_ref()));
    }
    if (edge.props_ref().is_set()) {
        edgeObj.insert("props", toJson(*edge.props_ref()));
    }
    return edgeObj;
}

folly::dynamic toJson(const storage::cpp2::UpdatedProp &prop) {
    return folly::dynamic::object("name", prop.get_name())("value", prop.get_value());
}

folly::dynamic toJson(const storage::cpp2::OrderBy &orderBy) {
    folly::dynamic obj = folly::dynamic::object();
    if (orderBy.direction_ref().is_set()) {
        auto dir = *orderBy.direction_ref();
        obj.insert("direction", apache::thrift::util::enumNameSafe(dir));
    }
    if (orderBy.prop_ref().is_set()) {
        obj.insert("prop", *orderBy.prop_ref());
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::VertexProp &prop) {
    folly::dynamic obj = folly::dynamic::object();
    if (prop.tag_ref().is_set()) {
        auto tag = *prop.tag_ref();
        obj.insert("tagId", tag);
    }
    if (prop.props_ref().is_set()) {
        obj.insert("props", toJson(*prop.props_ref()));
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::EdgeProp &prop) {
    folly::dynamic obj = folly::dynamic::object();
    if (prop.type_ref().is_set()) {
        obj.insert("type", toJson(*prop.type_ref()));
    }
    if (prop.props_ref().is_set()) {
        obj.insert("props", toJson(*prop.props_ref()));
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::StatProp &prop) {
    folly::dynamic obj = folly::dynamic::object();
    if (prop.alias_ref().is_set()) {
        obj.insert("alias", *prop.alias_ref());
    }
    if (prop.prop_ref().is_set()) {
        obj.insert("prop", *prop.prop_ref());
    }
    if (prop.stat_ref().is_set()) {
        obj.insert("stat", apache::thrift::util::enumNameSafe(*prop.stat_ref()));
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::Expr &expr) {
    folly::dynamic obj = folly::dynamic::object();
    if (expr.alias_ref().is_set()) {
        obj.insert("alias", *expr.alias_ref());
    }
    if (expr.expr_ref().is_set()) {
        obj.insert("expr", *expr.expr_ref());
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::IndexQueryContext &iqc) {
    folly::dynamic obj = folly::dynamic::object();
    obj.insert("index_id", iqc.get_index_id());
    ObjectPool tempPool;
    auto filter =
        iqc.get_filter().empty() ? "" : Expression::decode(&tempPool, iqc.get_filter())->toString();
    obj.insert("filter", filter);
    obj.insert("columnHints", toJson(iqc.get_column_hints()));
    return obj;
}

folly::dynamic toJson(const storage::cpp2::IndexColumnHint &hints) {
    folly::dynamic obj = folly::dynamic::object();
    obj.insert("column", hints.get_column_name());
    auto scanType = apache::thrift::util::enumNameSafe(hints.get_scan_type());
    obj.insert("scanType", scanType);
    auto rtrim = [](const std::string &str) { return std::string(str.c_str()); };
    auto begin = toJson(hints.get_begin_value());
    obj.insert("beginValue", rtrim(begin));
    auto end = toJson(hints.get_end_value());
    obj.insert("endValue", rtrim(end));
    return obj;
}

folly::dynamic toJson(const graph::Variable *var) {
    folly::dynamic obj = folly::dynamic::object();
    if (var == nullptr) {
        return obj;
    }
    obj.insert("name", var->name);
    std::stringstream ss;
    ss << var->type;
    obj.insert("type", ss.str());
    obj.insert("colNames", toJson(var->colNames));
    return obj;
}
}   // namespace util
}   // namespace nebula
