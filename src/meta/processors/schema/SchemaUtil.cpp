/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/SchemaUtil.h"

#include "common/base/ObjectPool.h"
#include "common/expression/ConstantExpression.h"
#include "common/time/TimeUtils.h"
#include "common/utils/DefaultValueContext.h"

namespace nebula {
namespace meta {

using nebula::cpp2::PropertyType;

bool SchemaUtil::checkType(std::vector<cpp2::ColumnDef>& columns) {
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
        case PropertyType::BOOL:
          if (!value.isBool()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::INT8: {
          if (!value.isInt()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }

          auto v = value.getInt();
          if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
            LOG(ERROR) << "`" << name << "'  out of rang";
            return false;
          }
          break;
        }
        case PropertyType::INT16: {
          if (!value.isInt()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }

          auto v = value.getInt();
          if (v > std::numeric_limits<int16_t>::max() || v < std::numeric_limits<int16_t>::min()) {
            LOG(ERROR) << "`" << name << "'  out of rang";
            return false;
          }
          break;
        }
        case PropertyType::INT32: {
          if (!value.isInt()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }

          auto v = value.getInt();
          if (v > std::numeric_limits<int32_t>::max() || v < std::numeric_limits<int32_t>::min()) {
            LOG(ERROR) << "`" << name << "'  out of rang";
            return false;
          }
          break;
        }
        case PropertyType::INT64:
          if (!value.isInt()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::FLOAT:
        case PropertyType::DOUBLE:
          if (!value.isFloat()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::STRING:
          if (!value.isStr()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::FIXED_STRING: {
          if (!value.isStr()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          auto& colType = column.get_type();
          size_t typeLen = colType.type_length_ref().value_or(0);
          if (value.getStr().size() > typeLen) {
            const auto trimStr = value.getStr().substr(0, typeLen);
            value.setStr(trimStr);
            const auto& fixedValue = *ConstantExpression::make(pool, value);
            column.default_value_ref() = Expression::encode(fixedValue);
          }
          break;
        }
        case PropertyType::TIMESTAMP: {
          if (!value.isInt()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          auto ret = time::TimeUtils::toTimestamp(value);
          if (!ret.ok()) {
            LOG(ERROR) << ret.status();
            return false;
          }
          break;
        }
        case PropertyType::DATE:
          if (!value.isDate()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::TIME:
          if (!value.isTime()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::DATETIME:
          if (!value.isDateTime()) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          break;
        case PropertyType::GEOGRAPHY: {
          if (!value.isGeography()) {  // TODO(jie)
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type();
            return false;
          }
          meta::cpp2::GeoShape columnGeoShape =
              column.get_type().geo_shape_ref().value_or(meta::cpp2::GeoShape::ANY);
          GeoShape defaultExprGeoShape = value.getGeography().shape();
          if (columnGeoShape != meta::cpp2::GeoShape::ANY &&
              folly::to<uint32_t>(columnGeoShape) != folly::to<uint32_t>(defaultExprGeoShape)) {
            LOG(ERROR) << "Invalid default value for ` " << name << "', value type is "
                       << value.type() << ", geo shape is " << defaultExprGeoShape;
            return false;
          }
          break;
        }
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
