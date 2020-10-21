/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ConstantExpression.h"

#include "meta/processors/schemaMan/SchemaUtil.h"
#include "utils/DefaultValueContext.h"

namespace nebula {
namespace meta {
bool SchemaUtil::checkType(std::vector<cpp2::ColumnDef> &columns) {
    DefaultValueContext mContext;
    for (auto& column : columns) {
        if (column.__isset.default_value) {
            auto name = column.get_name();
            auto defaultValueExpr = Expression::decode(*column.get_default_value());
            if (defaultValueExpr == nullptr) {
                LOG(ERROR) << "Wrong format default value expression for column name: " << name;
                return false;
            }
            auto value = Expression::eval(defaultValueExpr.get(), mContext);
            auto nullable = column.__isset.nullable ? *column.get_nullable() : false;
            if (nullable && value.isNull()) {
                continue;;
            }
            switch (column.get_type().get_type()) {
                case cpp2::PropertyType::BOOL:
                    if (value.type() != nebula::Value::Type::BOOL) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::INT8: {
                    if (value.type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }

                    auto v = value.getInt();
                    if (v > std::numeric_limits<int8_t>::max()
                            || v < std::numeric_limits<int8_t>::min()) {
                        LOG(ERROR) << "`" << name << "'  out of rang";
                        return false;
                    }
                    break;
                }
                case cpp2::PropertyType::INT16: {
                    if (value.type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }

                    auto v = value.getInt();
                    if (v > std::numeric_limits<int16_t>::max()
                            || v < std::numeric_limits<int16_t>::min()) {
                        LOG(ERROR) << "`" << name << "'  out of rang";
                        return false;
                    }
                    break;
                }
                case cpp2::PropertyType::INT32: {
                    if (value.type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }

                    auto v = value.getInt();
                    if (v > std::numeric_limits<int32_t>::max()
                            || v < std::numeric_limits<int32_t>::min()) {
                        LOG(ERROR) << "`" << name << "'  out of rang";
                        return false;
                    }
                    break;
                }
                case cpp2::PropertyType::INT64:
                    if (value.type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::FLOAT:
                case cpp2::PropertyType::DOUBLE:
                    if (value.type() != nebula::Value::Type::FLOAT) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::STRING:
                    if (value.type() != nebula::Value::Type::STRING) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::FIXED_STRING: {
                    if (value.type() != nebula::Value::Type::STRING) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    auto& colType = column.get_type();
                    size_t typeLen = colType.__isset.type_length ? *colType.get_type_length() : 0;
                    if (value.getStr().size() > typeLen) {
                        const auto trimStr = value.getStr().substr(0, typeLen);
                        value.setStr(trimStr);
                        ConstantExpression fixedValue(value);
                        column.set_default_value(Expression::encode(fixedValue));
                    }
                    break;
                }
                case cpp2::PropertyType::TIMESTAMP:
                    if (value.type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::DATE:
                    if (value.type() != nebula::Value::Type::DATE) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::DATETIME:
                    if (value.type() != nebula::Value::Type::DATETIME) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                default:
                    LOG(ERROR) << "Unsupported type";
                    return false;
            }
        }
    }
    return true;
}
}  // namespace meta
}  // namespace nebula
