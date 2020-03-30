/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/Utils.h"

namespace nebula {
namespace meta {

cpp2::ErrorCode checkDefaultValueType(const std::vector<nebula::cpp2::ColumnDef> &columns) {
    for (auto& column : columns) {
        if (column.__isset.default_value) {
            auto name = column.get_name();
            auto value = column.get_default_value();
            switch (column.get_type().get_type()) {
                case nebula::cpp2::SupportedType::BOOL:
                    if (value->getType() != nebula::cpp2::Value::Type::bool_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return cpp2::ErrorCode::E_CONFLICT;
                    }
                    break;
                case nebula::cpp2::SupportedType::INT:
                    if (value->getType() != nebula::cpp2::Value::Type::int_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return cpp2::ErrorCode::E_CONFLICT;
                    }
                    break;
                case nebula::cpp2::SupportedType::DOUBLE:
                    if (value->getType() != nebula::cpp2::Value::Type::double_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return cpp2::ErrorCode::E_CONFLICT;
                    }
                    break;
                case nebula::cpp2::SupportedType::STRING:
                    if (value->getType() != nebula::cpp2::Value::Type::string_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return cpp2::ErrorCode::E_CONFLICT;
                    }
                    break;
                case nebula::cpp2::SupportedType::TIMESTAMP:
                    if (value->getType() != nebula::cpp2::Value::Type::timestamp) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return cpp2::ErrorCode::E_CONFLICT;
                    }
                    break;
                default:
                    LOG(ERROR) << "Unsupported type";
                    return cpp2::ErrorCode::E_CONFLICT;
            }
        }  // if
    }  // for
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
