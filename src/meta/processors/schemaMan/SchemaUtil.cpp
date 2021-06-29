/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/ObjectPool.h"
#include "common/expression/ConstantExpression.h"
#include "common/time/TimeUtils.h"

#include "meta/processors/schemaMan/SchemaUtil.h"
#include "utils/DefaultValueContext.h"

namespace nebula {
namespace meta {
bool SchemaUtil::checkType(std::vector<cpp2::ColumnDef> &columns) {
    DefaultValueContext mContext;
    ObjectPool Objpool;
    auto pool = &Objpool;

    for (auto& column : columns) {
        if (column.default_value_ref().has_value()) {
            auto name = column.get_name();
            auto defaultValueExpr = Expression::decode(pool, *column.default_value_ref());
            if (defaultValueExpr == nullptr) {
                LOG(ERROR) << "Wrong format default value expression for column name: " << name;
                return false;
            }
            auto value = Expression::eval(defaultValueExpr, mContext);
            auto nullable = column.nullable_ref().value_or(false);
            if (nullable && value.isNull()) {
                if (value.getNull() != NullType::__NULL__) {
                    LOG(ERROR) << "Invalid default value for `" << name
                               << "', it's the wrong null type: " << value.getNull();
                    return false;
                }
                continue;
            }
            switch (column.get_type().get_type()) {
                case cpp2::PropertyType::BOOL:
                    if (!value.isBool()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::INT8: {
                    if (!value.isInt()) {
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
                    if (!value.isInt()) {
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
                    if (!value.isInt()) {
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
                    if (!value.isInt()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::FLOAT:
                case cpp2::PropertyType::DOUBLE:
                    if (!value.isFloat()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::STRING:
                    if (!value.isStr()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::FIXED_STRING: {
                    if (!value.isStr()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    auto &colType = column.get_type();
                    size_t typeLen = colType.type_length_ref().value_or(0);
                    if (value.getStr().size() > typeLen) {
                        const auto trimStr = value.getStr().substr(0, typeLen);
                        value.setStr(trimStr);
                        const auto& fixedValue = *ConstantExpression::make(pool, value);
                        column.set_default_value(Expression::encode(fixedValue));
                    }
                    break;
                }
                case cpp2::PropertyType::TIMESTAMP: {
                    if (!value.isInt()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    auto ret = time::TimeUtils::toTimestamp(value);
                    if (!ret.ok()) {
                        LOG(ERROR) << ret.status();
                        return false;
                    }
                    break;
                }
                case cpp2::PropertyType::DATE:
                    if (!value.isDate()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::TIME:
                    if (!value.isTime()) {
                        LOG(ERROR) << "Invalid default value for ` " << name
                                   << "', value type is " << value.type();
                        return false;
                    }
                    break;
                case cpp2::PropertyType::DATETIME:
                    if (!value.isDateTime()) {
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

