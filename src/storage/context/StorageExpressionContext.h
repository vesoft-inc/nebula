/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_
#define STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_

#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/context/ExpressionContext.h"
#include "common/datatypes/Value.h"
#include "codec/RowReader.h"

namespace nebula {
namespace storage {

/*
StorageExpressionContext supports both read value from a RowReader, or user set value by
`setTagProp` and `setEdgeProp`.

If we need to read value from the RowReader, be sure to set the reader by calling `reset`. For now,
it only supports read from **one row**. So the reader is either a row of vertex or a row of edge.
This mode is used in GetNeighbors at present.

If we need to read value from a user defined place, just set the related value by
`setTagProp` and `setEdgeProp`. Be sure about not pass the RowReader by `reset`.
*/
class StorageExpressionContext final : public ExpressionContext {
public:
    explicit StorageExpressionContext(size_t vIdLen,
                                      bool isIntId,
                                      const std::string& name = "",
                                      const meta::NebulaSchemaProvider* schema = nullptr,
                                      bool isEdge = false)
        : vIdLen_(vIdLen)
        , isIntId_(isIntId)
        , name_(name)
        , schema_(schema)
        , isEdge_(isEdge) {}

    StorageExpressionContext(size_t vIdLen,
                             bool isIntId,
                             bool hasNullableCol,
                             const std::vector<meta::cpp2::ColumnDef>& fields)
        : vIdLen_(vIdLen)
        , isIntId_(isIntId)
        , hasNullableCol_(hasNullableCol)
        , fields_(fields) {
        isIndex_ = true;
    }

    // Get the latest version value for the given variable name, such as $a, $b
    const Value& getVar(const std::string& name) const override {
        auto it = valueMap_.find(name);
        if (it != valueMap_.end() && !it->second.empty()) {
            return it->second.back();
        } else {
            return Value::kNullValue;
        }
    }

    // Get the given version value for the given variable name, such as $a, $b
    const Value& getVersionedVar(const std::string&, int64_t) const override {
        return Value::kNullValue;
    }

    // Get the specified property from a variable, such as $a.prop_name
    const Value& getVarProp(const std::string&, const std::string&) const override {
        return Value::kNullValue;
    }

    // Get the specified property of tagName from the destination vertex,
    // such as $$.tagName.prop_name
    const Value& getDstProp(const std::string&, const std::string&) const override {
        return Value::kNullValue;
    }

    // Get the specified property from the input, such as $-.prop_name
    const Value& getInputProp(const std::string&) const override {
        return Value::kNullValue;
    }

    // Get the value by column index
    Value getColumn(int32_t) const override {
        return Value::kNullValue;
    }

    // Get the specified property from the tag, such as tag.prop_name
    Value getTagProp(const std::string& tagName, const std::string& prop) const override;

    // Get the specified property from the edge, such as edgename.prop_name
    Value getEdgeProp(const std::string& edgeName, const std::string& prop) const override;

    // Get the specified property of tagName from the source vertex,
    // such as $^.tagName.prop_name
    Value getSrcProp(const std::string& tagName, const std::string& prop) const override;

    size_t vIdLen() const {
        return vIdLen_;
    }

    bool hasNullableCol() const {
        return hasNullableCol_;
    }

    const std::vector<meta::cpp2::ColumnDef>& indexCols() const {
        return fields_;
    }

    void setVar(const std::string& name, Value val) override {
        valueMap_[name].emplace_back(std::move(val));
    }

    Value getVertex() const override {
        LOG(FATAL) << "Unimplemented";
    }

    Value getEdge() const override {
        LOG(FATAL) << "Unimplemented";
    }


    // index key
    void reset(const std::string& key) {
        key_ = key;
    }

    // isEdge_ set in ctor
    void reset(RowReader* reader,
               const std::string& key) {
        reader_ = reader;
        key_ = key;
    }

    void reset() {
        reader_ = nullptr;
        key_ = "";
        name_ = "";
        schema_ = nullptr;
    }

    void resetSchema(const std::string& name,
                     const meta::NebulaSchemaProvider* schema,
                     bool isEdge) {
        name_ = name;
        schema_ = schema;
        isEdge_ = isEdge;
    }

    void setTagProp(const std::string& tagName,
                    const std::string& prop,
                    nebula::Value value) {
        tagFilters_[std::make_pair(tagName, prop)] = std::move(value);
    }

    void setEdgeProp(const std::string& edgeName,
                     const std::string& prop,
                     nebula::Value value) {
        edgeFilters_[std::make_pair(edgeName, prop)] = std::move(value);
    }

    void clear() {
        tagFilters_.clear();
        edgeFilters_.clear();
    }

    Value readValue(const std::string& propName) const;

    Value getIndexValue(const std::string& prop, bool isEdge) const;

private:
    size_t                             vIdLen_;
    bool                               isIntId_;

    RowReader                         *reader_;
    std::string                        key_;
    // tag or edge name
    std::string                        name_;
    // tag or edge latest schema
    const meta::NebulaSchemaProvider  *schema_;
    bool                               isEdge_;

    // index
    bool isIndex_ = false;
    bool hasNullableCol_ = false;

    std::vector<meta::cpp2::ColumnDef> fields_;

    // <tagName, property> -> value
    std::unordered_map<std::pair<std::string, std::string>, nebula::Value> tagFilters_;

    // <edgeName, property> -> value
    std::unordered_map<std::pair<std::string, std::string>, nebula::Value> edgeFilters_;

    // name -> Value with multiple versions
    std::unordered_map<std::string, std::vector<Value>> valueMap_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_
