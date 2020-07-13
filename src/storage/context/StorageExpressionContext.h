/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_
#define STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "common/datatypes/Value.h"
#include "codec/RowReader.h"

namespace nebula {
namespace storage {

class StorageExpressionContext final : public ExpressionContext {
public:
    explicit StorageExpressionContext(size_t vIdLen)
        : vIdLen_(vIdLen) {}

    void reset(RowReader* reader, folly::StringPiece key, folly::StringPiece edgeName) {
        reader_ = reader;
        key_ = key;
        name_ = edgeName;
    }

    // Get the latest version value for the given variable name, such as $a, $b
    const Value& getVar(const std::string&) const override {
        return Value::kNullValue;
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

    // Get the specified property from the edge, such as edgename.prop_name
    Value getEdgeProp(const std::string& edgeName, const std::string& prop) const override;

    // Get the specified property of tagName from the source vertex,
    // such as $^.tagName.prop_name
    Value getSrcProp(const std::string& tagName, const std::string& prop) const override;

    void setVar(const std::string&, Value) override {}

    void setTagProp(const std::string& tagName,
                    const std::string& prop,
                    const nebula::Value& value) {
        tagFilters_.emplace(std::make_pair(tagName, prop), value);
    }

    void clear() {
        tagFilters_.clear();
    }

private:
    size_t vIdLen_;

    folly::StringPiece key_;
    RowReader* reader_;
    // tag or edge name
    folly::StringPiece name_;

    // <tagName, property> -> value
    std::unordered_map<std::pair<std::string, std::string>, nebula::Value> tagFilters_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_CONTEXT_STORAGEEXPRESSIONCONTEXT_H_
