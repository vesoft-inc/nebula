/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_ROWUPDATER_H_
#define DATAMAN_ROWUPDATER_H_


#include "base/Base.h"
#include "gen-cpp2/graph_types.h"
#include "dataman/DataCommon.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {

/**
 * This class is mainly used to update an existing row. The schema has
 * to be provided. The existing row is optional. If not provided, a default
 * value will be used for any un-assigned field
 */
class RowUpdater {
public:
    // reader holds the original data
    // schema is the writer schema, which means the updated data will be encoded
    //   using this schema
    RowUpdater(RowReader reader,
               std::shared_ptr<const meta::SchemaProviderIf> schema);
    explicit RowUpdater(std::shared_ptr<const meta::SchemaProviderIf> schema);

    // Encode into a binary array
    StatusOr<std::string> encode() const noexcept;
    // Encode and append to the given string
    // For the sake of performance, the caller needs to make sure the string
    // is large enough, so that resize will not happen
    Status encodeTo(std::string& encoded) const noexcept;

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

    Status writeDefaultValue(const folly::StringPiece name, RowWriter &writer) const noexcept;

    std::shared_ptr<const meta::SchemaProviderIf> schema() const {
        return schema_;
    }

    // TODO getPath(const std::string& name) const noexcept;
    // TODO getList(const std::string& name) const noexcept;
    // TODO getSet(const std::string& name) const noexcept;
    // TODO getMap(const std::string& name) const noexcept;

private:
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    RowReader reader_ = RowReader::getEmptyRowReader();
    // Hash64(field_name) => value
    std::unordered_map<uint64_t, FieldValue> updatedFields_;
};

}  // namespace nebula


#define RU_GET_TYPE_BY_NAME() \
    const cpp2::ValueType& type \
        = schema_->getFieldType(name); \
    if (type == CommonConstants::kInvalidValueType()) { \
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

#define RU_OUTPUT_VALUE(VT, FN) \
    VT val; \
    auto res = get ## FN(it->getName(), val); \
    if (res != ResultType::SUCCEEDED) { \
        if (res == ResultType::E_NAME_NOT_FOUND) { \
            /* Use a default value */ \
            ret = writeDefaultValue(it->getName(), writer); \
        } else { \
            LOG(ERROR) << "Failed to encode updated data"; \
            return Status::Error("Failed to encode updated data"); \
        } \
    } else { \
        writer << val; \
        ret = Status::OK(); \
    }

#include "dataman/RowUpdater.inl"

#endif  // DATAMAN_ROWUPDATER_H_


