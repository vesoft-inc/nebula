/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ToJson.h"

#include "common/clients/meta/MetaClient.h"
#include "common/datatypes/Value.h"
#include "common/expression/Expression.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/ColumnTypeDef.h"
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
    obj.insert("name", desc.space_name);
    obj.insert("partNum", desc.partition_num);
    obj.insert("replicaFactor", desc.replica_factor);
    obj.insert("charset", desc.charset_name);
    obj.insert("collate", desc.collate_name);
    auto vidSize = desc.vid_type.__isset.type_length ? *desc.vid_type.get_type_length() : 0;
    obj.insert("vidType", ColumnTypeDef(desc.vid_type.get_type(), vidSize).toString());
    return obj;
}

folly::dynamic toJson(const meta::cpp2::ColumnDef &column) {
    folly::dynamic obj = folly::dynamic::object();
    obj.insert("name", column.get_name());
    obj.insert("type", meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(column.get_type().get_type()));
    if (column.type.__isset.type_length) {
        obj.insert("typeLength", folly::to<std::string>(*column.get_type().get_type_length()));
    }
    if (column.__isset.nullable) {
        obj.insert("nullable", folly::to<std::string>(*column.get_nullable()));
    }
    if (column.__isset.default_value) {
        obj.insert("defaultValue", column.get_default_value()->toString());
    }
    return obj;
}

folly::dynamic toJson(const meta::cpp2::Schema &schema) {
    folly::dynamic json = folly::dynamic::object();
    if (schema.__isset.columns) {
        json.insert("columns", toJson(schema.get_columns()));
    }
    if (schema.__isset.schema_prop) {
        json.insert("prop", toJson(schema.get_schema_prop()));
    }
    return json;
}

folly::dynamic toJson(const meta::cpp2::SchemaProp &prop) {
    folly::dynamic object = folly::dynamic::object();
    if (prop.__isset.ttl_col) {
        object.insert("ttlCol", *prop.get_ttl_col());
    }
    if (prop.__isset.ttl_duration) {
        object.insert("ttlDuration", *prop.get_ttl_duration());
    }
    return object;
}

folly::dynamic toJson(const meta::cpp2::AlterSchemaItem &item) {
    folly::dynamic json = folly::dynamic::object();
    if (item.__isset.schema) {
        json.insert("schema", toJson(item.get_schema()));
    }
    if (item.__isset.op) {
        json.insert("op", meta::cpp2::_AlterSchemaOp_VALUES_TO_NAMES.at(item.get_op()));
    }
    return json;
}

folly::dynamic toJson(const storage::cpp2::EdgeKey &edgeKey) {
    folly::dynamic edgeKeyObj = folly::dynamic::object();
    if (edgeKey.__isset.src) {
        edgeKeyObj.insert("src", toJson(edgeKey.get_src()));
    }
    if (edgeKey.__isset.dst) {
        edgeKeyObj.insert("dst", toJson(edgeKey.get_dst()));
    }
    if (edgeKey.__isset.edge_type) {
        edgeKeyObj.insert("edgeType", edgeKey.get_edge_type());
    }
    if (edgeKey.__isset.ranking) {
        edgeKeyObj.insert("ranking", edgeKey.get_ranking());
    }
    return edgeKeyObj;
}

folly::dynamic toJson(const storage::cpp2::NewTag &tag) {
    folly::dynamic tagObj = folly::dynamic::object();
    if (tag.__isset.tag_id) {
        tagObj.insert("tagId", tag.get_tag_id());
    }
    if (tag.__isset.props) {
        tagObj.insert("props", toJson(tag.get_props()));
    }
    return tagObj;
}

folly::dynamic toJson(const storage::cpp2::NewVertex &vert) {
    folly::dynamic vertObj = folly::dynamic::object();
    if (vert.__isset.id) {
        vertObj.insert("id", toJson(vert.get_id()));
    }
    if (vert.__isset.tags) {
        vertObj.insert("tags", util::toJson(vert.get_tags()));
    }
    return vertObj;
}

folly::dynamic toJson(const storage::cpp2::NewEdge &edge) {
    folly::dynamic edgeObj = folly::dynamic::object();
    if (edge.__isset.key) {
        edgeObj.insert("key", toJson(edge.get_key()));
    }
    if (edge.__isset.props) {
        edgeObj.insert("props", toJson(edge.get_props()));
    }
    return edgeObj;
}

folly::dynamic toJson(const storage::cpp2::UpdatedProp &prop) {
    return folly::dynamic::object("name", prop.get_name())("value", prop.get_value());
}

folly::dynamic toJson(const storage::cpp2::OrderBy &orderBy) {
    folly::dynamic obj = folly::dynamic::object();
    if (orderBy.__isset.direction) {
        auto dir = orderBy.get_direction();
        obj.insert("direction", storage::cpp2::_OrderDirection_VALUES_TO_NAMES.at(dir));
    }
    if (orderBy.__isset.prop) {
        obj.insert("prop", orderBy.get_prop());
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::VertexProp &prop) {
    folly::dynamic obj = folly::dynamic::object();
    if (prop.__isset.tag) {
        auto tag = prop.get_tag();
        obj.insert("tagId", tag);
    }
    if (prop.__isset.props) {
        obj.insert("props", toJson(prop.get_props()));
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::EdgeProp &prop) {
    folly::dynamic obj = folly::dynamic::object();
    if (prop.__isset.type) {
        obj.insert("type", toJson(prop.get_type()));
    }
    if (prop.__isset.props) {
        obj.insert("props", toJson(prop.get_props()));
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::StatProp &prop) {
    folly::dynamic obj = folly::dynamic::object();
    if (prop.__isset.alias) {
        obj.insert("alias", prop.get_alias());
    }
    if (prop.__isset.prop) {
        obj.insert("prop", prop.get_prop());
    }
    if (prop.__isset.stat) {
        obj.insert("stat", storage::cpp2::_StatType_VALUES_TO_NAMES.at(prop.get_stat()));
    }
    return obj;
}

folly::dynamic toJson(const storage::cpp2::Expr &expr) {
    folly::dynamic obj = folly::dynamic::object();
    if (expr.__isset.alias) {
        obj.insert("alias", expr.get_alias());
    }
    if (expr.__isset.expr) {
        obj.insert("expr", expr.get_expr());
    }
    return obj;
}

}   // namespace util
}   // namespace nebula
