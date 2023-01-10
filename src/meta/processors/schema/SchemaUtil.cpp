/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/SchemaUtil.h"

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
        LOG(INFO) << "Wrong format default value expression for column name: " << name;
        return false;
      }
      auto value = Expression::eval(defaultValueExpr, mContext);
      auto nullable = column.nullable_ref().value_or(false);
      if (nullable && value.isNull()) {
        if (value.getNull() != NullType::__NULL__) {
          LOG(INFO) << "Invalid default value for `" << name
                    << "', it's the wrong null type: " << value.getNull();
          return false;
        }
        continue;
      }
      if (!checkType(pool, name, column, value)) {
        return false;
      }
    }
  }
  return true;
}

/*static*/ bool SchemaUtil::checkType(ObjectPool* pool,
                                      const std::string& name,
                                      cpp2::ColumnDef& column,
                                      const Value& value) {
  switch (column.get_type().get_type()) {
    case PropertyType::BOOL:
      if (!value.isBool()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::INT8: {
      if (!value.isInt()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }

      auto v = value.getInt();
      if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
        LOG(INFO) << "`" << name << "'  out of rang";
        return false;
      }
      return true;
    }
    case PropertyType::INT16: {
      if (!value.isInt()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }

      auto v = value.getInt();
      if (v > std::numeric_limits<int16_t>::max() || v < std::numeric_limits<int16_t>::min()) {
        LOG(INFO) << "`" << name << "'  out of rang";
        return false;
      }
      return true;
    }
    case PropertyType::INT32: {
      if (!value.isInt()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }

      auto v = value.getInt();
      if (v > std::numeric_limits<int32_t>::max() || v < std::numeric_limits<int32_t>::min()) {
        LOG(INFO) << "`" << name << "'  out of rang";
        return false;
      }
      return true;
    }
    case PropertyType::INT64:
      if (!value.isInt()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::FLOAT:
    case PropertyType::DOUBLE:
      if (!value.isFloat()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::STRING:
      if (!value.isStr()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::FIXED_STRING: {
      if (!value.isStr()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      auto& colType = column.get_type();
      size_t typeLen = colType.type_length_ref().value_or(0);
      if (value.getStr().size() > typeLen) {
        const auto trimStr = value.getStr().substr(0, typeLen);
        const auto& fixedValue = *ConstantExpression::make(pool, Value(trimStr));
        column.default_value_ref() = Expression::encode(fixedValue);
      }
      return true;
    }
    case PropertyType::TIMESTAMP: {
      if (!value.isInt()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      auto ret = time::TimeUtils::toTimestamp(value);
      if (!ret.ok()) {
        LOG(INFO) << ret.status();
        return false;
      }
      return true;
    }
    case PropertyType::DATE:
      if (!value.isDate()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::TIME:
      if (!value.isTime()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::DATETIME:
      if (!value.isDateTime()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::DURATION:
      if (!value.isDuration()) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      return true;
    case PropertyType::GEOGRAPHY: {
      if (!value.isGeography()) {  // TODO(jie)
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type();
        return false;
      }
      meta::cpp2::GeoShape columnGeoShape =
          column.get_type().geo_shape_ref().value_or(meta::cpp2::GeoShape::ANY);
      GeoShape defaultExprGeoShape = value.getGeography().shape();
      if (columnGeoShape != meta::cpp2::GeoShape::ANY &&
          folly::to<uint32_t>(columnGeoShape) != folly::to<uint32_t>(defaultExprGeoShape)) {
        LOG(INFO) << "Invalid default value for ` " << name << "', value type is " << value.type()
                  << ", geo shape is " << defaultExprGeoShape;
        return false;
      }
      return true;
    }
    case PropertyType::UNKNOWN:
    case PropertyType::VID:
      DLOG(INFO) << "Don't supported type "
                 << apache::thrift::util::enumNameSafe(column.get_type().get_type());
      return false;
      // no default so compiler will warning when lack
  }  // switch
  DLOG(FATAL) << "Unknown property type " << static_cast<int>(column.get_type().get_type());
  return false;
}

}  // namespace meta
}  // namespace nebula
