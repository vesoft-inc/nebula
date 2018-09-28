/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_ROWUPDATER_H_
#define DATAMAN_ROWUPDATER_H_


#include "base/Base.h"
#include "interface/gen-cpp2/vgraph_types.h"
#include "dataman/DataCommon.h"
#include "dataman/SchemaProviderIf.h"
#include "dataman/RowReader.h"

namespace vesoft {
namespace vgraph {

/**
 * This class is mainly used to update an existing row. The schema has
 * to be provided. The existing row is optional. If not provided, a default
 * value will be used for any un-assigned field
 */
class RowUpdater {
public:
    explicit RowUpdater(SchemaProviderIf* schema,
                        folly::StringPiece row = folly::StringPiece());

    // Encode into a binary array
    std::string encode() const noexcept;
    // Encode and append to the given string
    // For the sake of performance, the caller needs to make sure the string
    // is large enough, so that resize will not happen
    void encodeTo(std::string& encoded) const noexcept;

    /**
     * Accessors
     */
    ResultType setBool(const folly::StringPiece name, bool v) noexcept;
    ResultType getBool(const folly::StringPiece name, bool& v) const noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    setInt(const folly::StringPiece name, T v) noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    getInt(const folly::StringPiece name, T& v) const noexcept;

    ResultType setFloat(const folly::StringPiece name, float v) noexcept;
    ResultType getFloat(const folly::StringPiece name, float& v) const noexcept;

    ResultType setDouble(const folly::StringPiece name, double v) noexcept;
    ResultType getDouble(const folly::StringPiece name, double& v) const noexcept;

    ResultType setString(const folly::StringPiece name,
                         folly::StringPiece v) noexcept;
    ResultType getString(const folly::StringPiece name,
                         folly::StringPiece& v) const noexcept;

    ResultType setVid(const folly::StringPiece name, int64_t v) noexcept;
    ResultType getVid(const folly::StringPiece name, int64_t& v) const noexcept;

    // TODO getPath(const std::string& name) const noexcept;
    // TODO getList(const std::string& name) const noexcept;
    // TODO getSet(const std::string& name) const noexcept;
    // TODO getMap(const std::string& name) const noexcept;

private:
    SchemaProviderIf* schema_;
    int32_t schemaVer_;
    std::unique_ptr<RowReader> reader_;
    // Hash64(field_name) => value
    std::unordered_map<uint64_t, FieldValue> updatedFields_;
};

}  // namespace vgraph
}  // namespace vesoft


#define RU_GET_TYPE_BY_NAME() \
    const cpp2::ValueType* type = schema_->getFieldType(name, schemaVer_); \
    if (!type) { \
        return ResultType::E_NAME_NOT_FOUND; \
    }


#define RU_CHECK_UPDATED_FIELDS(FN)  \
    uint64_t hash = folly::hash::SpookyHashV2::Hash64(name.begin(), \
                                                      name.size(), \
                                                      0); \
    auto it = updatedFields_.find(hash); \
    if (it == updatedFields_.end()) { \
        if (!reader_) { \
            return ResultType::E_NAME_NOT_FOUND; \
        } else { \
            return reader_->get ## FN (name, v); \
        } \
    }

#define RU_OUTPUT_VALUE(VT, FN, DV) \
    VT val; \
    auto res = get ## FN (it->getName(), val); \
    if (res != ResultType::SUCCEEDED) { \
        if (res == ResultType::E_NAME_NOT_FOUND) { \
            /* Use a default value */ \
            writer << DV; \
        } else { \
            LOG(ERROR) << "Failed to encode updated data"; \
            return; \
        } \
    } else { \
        writer << val; \
    }

#include "dataman/RowUpdater.inl"

#endif  // DATAMAN_ROWUPDATER_H_


