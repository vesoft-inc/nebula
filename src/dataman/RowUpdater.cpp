/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman/RowUpdater.h"

namespace nebula {

using folly::hash::SpookyHashV2;
using nebula::meta::SchemaProviderIf;

RowUpdater::RowUpdater(RowReader reader,
                       std::shared_ptr<const meta::SchemaProviderIf> schema)
        : schema_(std::move(schema))
        , reader_(std::move(reader)) {
    CHECK(!!schema_);
}


RowUpdater::RowUpdater(std::shared_ptr<const meta::SchemaProviderIf> schema)
        : schema_(std::move(schema))
        , reader_(RowReader::getEmptyRowReader()) {
    CHECK(!!schema_);
}


StatusOr<std::string> RowUpdater::encode() const noexcept {
    std::string encoded;
    // TODO Reserve enough space so resize will not happen
    auto status = encodeTo(encoded);
    if (!status.ok()) {
        return status;
    }
    return encoded;
}

Status RowUpdater::encodeTo(std::string& encoded) const noexcept {
    RowWriter writer(schema_);
    auto it = schema_->begin();
    while (static_cast<bool>(it)) {
        Status ret = Status::OK();
        switch (it->getType().get_type()) {
            case cpp2::SupportedType::BOOL: {
                RU_OUTPUT_VALUE(bool, Bool);
                break;
            }
            case cpp2::SupportedType::INT:
            case cpp2::SupportedType::TIMESTAMP: {
                RU_OUTPUT_VALUE(int64_t, Int);
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                RU_OUTPUT_VALUE(float, Float);
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                RU_OUTPUT_VALUE(double, Double);
                break;
            }
            case cpp2::SupportedType::STRING: {
                RU_OUTPUT_VALUE(folly::StringPiece, String);
                break;
            }
            case cpp2::SupportedType::VID: {
                RU_OUTPUT_VALUE(int64_t, Vid);
                break;
            }
            default: {
                LOG(FATAL) << "Unimplemented";
            }
        }
        if (!ret.ok()) {
            LOG(ERROR) << ret;
            return ret;
        }
        ++it;
    }
    writer.encodeTo(encoded);
    return Status::OK();
}


/***************************************************
 *
 * Field Updaters
 *
 **************************************************/
ResultType RowUpdater::setBool(const folly::StringPiece name,
                               bool v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type.get_type()) {
        case cpp2::SupportedType::BOOL:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setFloat(const folly::StringPiece name,
                                float v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type.get_type()) {
        case cpp2::SupportedType::FLOAT:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        case cpp2::SupportedType::DOUBLE:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = static_cast<double>(v);
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setDouble(const folly::StringPiece name,
                                 double v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type.get_type()) {
        case cpp2::SupportedType::FLOAT:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = static_cast<float>(v);
            break;
        case cpp2::SupportedType::DOUBLE:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setString(const folly::StringPiece name,
                                 folly::StringPiece v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type.get_type()) {
        case cpp2::SupportedType::STRING:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v.toString();
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setVid(const folly::StringPiece name,
                              int64_t v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type.get_type()) {
        case cpp2::SupportedType::VID:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}

Status RowUpdater::writeDefaultValue(const folly::StringPiece name,
                                     RowWriter &writer) const noexcept {
    auto status = schema_->getDefaultValue(name);
    if (!status.ok()) {
        LOG(ERROR) << status.status();
        return status.status();
    }
    auto value = std::move(status.value());
    switch (value.which()) {
        case VAR_INT64: {}
            writer << boost::get<int64_t>(value);
            return Status::OK();
        case VAR_DOUBLE:
            writer << boost::get<double>(value);
            return Status::OK();
        case VAR_BOOL:
            writer << boost::get<bool>(value);
            return Status::OK();
        case VAR_STR:
            writer << boost::get<std::string>(value);
            return Status::OK();
        default:
            LOG(ERROR) << "Unknown VariantType: " << value.which();
            return Status::Error("Unknown VariantType: %d", value.which());
    }
}

/***************************************************
 *
 * Field Accessors
 *
 **************************************************/
ResultType RowUpdater::getBool(const folly::StringPiece name,
                               bool& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Bool)

    switch (it->second.which()) {
    case VALUE_TYPE_BOOL:
        v = boost::get<bool>(it->second);
        break;
    case VALUE_TYPE_INT:
        v = intToBool(boost::get<int64_t>(it->second));
        break;
    case VALUE_TYPE_STRING:
        v = strToBool(boost::get<std::string>(it->second));
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getFloat(const folly::StringPiece name,
                                float& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Float)

    switch (it->second.which()) {
    case VALUE_TYPE_FLOAT:
        v = boost::get<float>(it->second);
        break;
    case VALUE_TYPE_DOUBLE:
        v = boost::get<double>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getDouble(const folly::StringPiece name,
                                 double& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Double)

    switch (it->second.which()) {
    case VALUE_TYPE_FLOAT:
        v = boost::get<float>(it->second);
        break;
    case VALUE_TYPE_DOUBLE:
        v = boost::get<double>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getString(const folly::StringPiece name,
                                 folly::StringPiece& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(String)

    switch (it->second.which()) {
    case VALUE_TYPE_STRING:
        v = boost::get<std::string>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getVid(const folly::StringPiece name,
                              int64_t& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Vid)

    switch (it->second.which()) {
    case VALUE_TYPE_INT:
        v = boost::get<int64_t>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


#undef CHECK_UPDATED_FIELDS

}  // namespace nebula
