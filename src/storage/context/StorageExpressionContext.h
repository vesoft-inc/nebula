/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_
#define STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_

#include "codec/RowReader.h"
#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/context/ExpressionContext.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace storage {

/**
 * @brief StorageExpressionContext supports both read value from a RowReader, or user set
 * value by `setTagProp` and `setEdgeProp`.
 * If we need to read value from the RowReader, be sure to set the reader by
 * calling `reset`. For now, it only supports read from **one row**. So the reader
 * is either a row of vertex or a row of edge. This mode is used in GetNeighbors at
 * present.
 * If we need to read value from a user defined place, just set the related value
 * by `setTagProp` and `setEdgeProp`. Be sure about not pass the RowReader by
 * `reset`.
 *
 */
class StorageExpressionContext final : public ExpressionContext {
 public:
  StorageExpressionContext(size_t vIdLen,
                           bool isIntId,
                           const std::string& name = "",
                           const meta::NebulaSchemaProvider* schema = nullptr,
                           bool isEdge = false)
      : vIdLen_(vIdLen), isIntId_(isIntId), name_(name), schema_(schema), isEdge_(isEdge) {}

  StorageExpressionContext(size_t vIdLen,
                           bool isIntId,
                           bool hasNullableCol,
                           const std::vector<meta::cpp2::ColumnDef>& fields)
      : vIdLen_(vIdLen), isIntId_(isIntId), hasNullableCol_(hasNullableCol), fields_(fields) {
    isIndex_ = true;
  }

  /**
   * @brief Get the latest version value for the given variable name, such as $a, $b
   *
   * @param name Given variable name.
   * @return const Value& Value's const reference of given names.
   */
  const Value& getVar(const std::string& name) const override {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end() && !it->second.empty()) {
      return it->second.back();
    } else {
      return Value::kNullValue;
    }
  }

  /**
   * @brief Get the given version value for the given variable name, such as $a, $b.
   *
   * @return const Value& Value's const reference of given name and version.
   */
  const Value& getVersionedVar(const std::string&, int64_t) const override {
    return Value::kNullValue;
  }

  /**
   * @brief Get the specified property from a variable, such as $a.prop_name.
   *
   * @return const Value& Value's const reference of variable's property.
   */
  const Value& getVarProp(const std::string&, const std::string&) const override {
    return Value::kNullValue;
  }

  /**
   * @brief Get the specified property of tagName from the destination vertex, such as
   * $$.tagName.prop_name
   *
   * @return const Value& Value's const reference of destination vertex's property.
   */
  const Value& getDstProp(const std::string&, const std::string&) const override {
    return Value::kNullValue;
  }

  /**
   * @brief Get the specified property from the input, such as $-.prop_name
   *
   * @return const Value&
   */
  const Value& getInputProp(const std::string&) const override {
    return Value::kNullValue;
  }

  /**
   * @brief Get the value by column index
   *
   * @return Value
   */
  Value getColumn(int32_t) const override {
    return Value::kNullValue;
  }

  /**
   * @brief Get the specified property from the tag, such as tag.prop_name
   *
   * @param tagName Given tag name.
   * @param prop Given property name.
   * @return Value
   */
  Value getTagProp(const std::string& tagName, const std::string& prop) const override;

  /**
   * @brief Get the specified property from the edge, such as edgename.prop_name
   *
   * @param edgeName Given edge name.
   * @param prop Given property name.
   * @return Value
   */
  Value getEdgeProp(const std::string& edgeName, const std::string& prop) const override;

  /**
   * @brief Get the specified property of tagName from the source vertex,
   * such as $^.tagName.prop_name
   *
   * @param tagName Given tag name.
   * @param prop Given property name.
   * @return Value
   */
  Value getSrcProp(const std::string& tagName, const std::string& prop) const override;

  /**
   * @brief Get vid length.
   *
   * @return size_t vid length.
   */
  size_t vIdLen() const {
    return vIdLen_;
  }

  /**
   * @brief check if nullable column exists.
   *
   * @return true nullable column exists.
   * @return false nullable column does not exist.
   */
  bool hasNullableCol() const {
    return hasNullableCol_;
  }

  /**
   * @brief Set the Var object by name.
   *
   * @param name Given variable name.
   * @param val Value to set.
   */
  void setVar(const std::string& name, Value val) override {
    valueMap_[name].emplace_back(std::move(val));
  }

  /**
   * @brief Get the Vertex object by name.
   *
   * @param name Given variable name.
   * @return Value Value to return.
   */
  Value getVertex(const std::string& name = "") const override {
    UNUSED(name);
    LOG(FATAL) << "Unimplemented";
  }

  /**
   * @brief Get the Edge object.
   *
   * @return Value
   */
  Value getEdge() const override {
    LOG(FATAL) << "Unimplemented";
  }

  /**
   * @brief Reset key string.
   *
   * @param key Key string to reset.
   */
  void reset(const std::string& key) {
    key_ = key;
  }

  /**
   * @brief Reset row reader and key string.
   *
   * @param reader Row reader to reset.
   * @param key Key string to reset.
   */
  void reset(RowReader* reader, const std::string& key) {
    reader_ = reader;
    key_ = key;
  }

  /**
   * @brief Empty reset.
   *
   */
  void reset() {
    reader_ = nullptr;
    key_ = "";
    name_ = "";
    schema_ = nullptr;
  }

  /**
   * @brief Reset schema
   *
   * @param name  Name string to reset.
   * @param schema Schema to reset.
   * @param isEdge Is edge flag.
   */
  void resetSchema(const std::string& name, const meta::NebulaSchemaProvider* schema, bool isEdge) {
    name_ = name;
    schema_ = schema;
    isEdge_ = isEdge;
  }

  /**
   * @brief Set the Tag Prop object
   *
   * @param tagName Tag name.
   * @param prop Porperty name.
   * @param value Value to set.
   */
  void setTagProp(const std::string& tagName, const std::string& prop, nebula::Value value) {
    tagFilters_[std::make_pair(tagName, prop)] = std::move(value);
  }

  /**
   * @brief Set the Edge Prop object
   *
   * @param edgeName Edge name.
   * @param prop Property name
   * @param value  Value to set.
   */
  void setEdgeProp(const std::string& edgeName, const std::string& prop, nebula::Value value) {
    edgeFilters_[std::make_pair(edgeName, prop)] = std::move(value);
  }

  /**
   * @brief Clear tag and edge filter.
   *
   */
  void clear() {
    tagFilters_.clear();
    edgeFilters_.clear();
  }

  /**
   * @brief Read value by property name.
   *
   * @param propName property name.
   * @return Value Value to read.
   */
  Value readValue(const std::string& propName) const;

  /**
   * @brief Get the Index Value object
   *
   * @param prop  Property name.
   * @param isEdge IsEdge flag.
   * @return Value Value to read.
   */
  Value getIndexValue(const std::string& prop, bool isEdge) const;

 private:
  size_t vIdLen_;
  bool isIntId_;

  RowReader* reader_{nullptr};
  std::string key_;
  // tag or edge name
  std::string name_;
  // tag or edge latest schema
  const meta::NebulaSchemaProvider* schema_;
  bool isEdge_;

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
