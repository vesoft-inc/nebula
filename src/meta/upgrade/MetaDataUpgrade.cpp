/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/upgrade/MetaDataUpgrade.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/conf/Configuration.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Value.h"
#include "common/expression/ConstantExpression.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/Common.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/upgrade/v2/MetaServiceUtilsV2.h"

DECLARE_bool(null_type);
DECLARE_uint32(string_index_limit);

namespace nebula {
namespace meta {

Status MetaDataUpgrade::rewriteSpacesV2ToV3(const folly::StringPiece &key,
                                            const folly::StringPiece &val) {
  auto oldProps = meta::v2::MetaServiceUtilsV2::parseSpace(val);
  cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = oldProps.get_space_name();
  spaceDesc.partition_num_ref() = oldProps.get_partition_num();
  spaceDesc.replica_factor_ref() = oldProps.get_replica_factor();
  spaceDesc.charset_name_ref() = oldProps.get_charset_name();
  spaceDesc.collate_name_ref() = oldProps.get_collate_name();
  cpp2::ColumnTypeDef def;
  auto &type = oldProps.get_vid_type();
  def.type_length_ref() = *type.get_type_length();
  def.type_ref() = convertToPropertyType(type.get_type());

  if (type.geo_shape_ref().has_value()) {
    def.geo_shape_ref() = convertToGeoShape(*type.get_geo_shape());
  }
  spaceDesc.vid_type_ref() = std::move(def);
  if (oldProps.isolation_level_ref().has_value()) {
    if (*oldProps.isolation_level_ref() == nebula::meta::v2::cpp2::IsolationLevel::DEFAULT) {
      spaceDesc.isolation_level_ref() = nebula::meta::cpp2::IsolationLevel::DEFAULT;
    } else {
      spaceDesc.isolation_level_ref() = nebula::meta::cpp2::IsolationLevel::TOSS;
    }
  }

  if (oldProps.comment_ref().has_value()) {
    spaceDesc.comment_ref() = *oldProps.comment_ref();
  }

  if (oldProps.group_name_ref().has_value()) {
    auto groupName = *oldProps.group_name_ref();
    auto groupKey = meta::v2::MetaServiceUtilsV2::groupKey(groupName);
    std::string zoneValue;
    auto code = engine_->get(std::move(groupKey), &zoneValue);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return Status::Error("Get Group Failed");
    }

    auto zones = meta::v2::MetaServiceUtilsV2::parseZoneNames(std::move(zoneValue));
    spaceDesc.zone_names_ref() = std::move(zones);
  } else {
    const auto &zonePrefix = MetaKeyUtils::zonePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = engine_->prefix(zonePrefix, &iter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return Status::Error("Get Zones Failed");
    }

    std::vector<::std::string> zones;
    while (iter->valid()) {
      auto zoneName = MetaKeyUtils::parseZoneName(iter->key());
      zones.emplace_back(std::move(zoneName));
      iter->next();
    }
    spaceDesc.zone_names_ref() = std::move(zones);
  }

  NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaKeyUtils::spaceVal(spaceDesc)));
  return Status::OK();
}

Status MetaDataUpgrade::deleteKeyVal(const folly::StringPiece &key) {
  NG_LOG_AND_RETURN_IF_ERROR(remove(key));
  return Status::OK();
}

nebula::cpp2::PropertyType MetaDataUpgrade::convertToPropertyType(
    nebula::meta::v2::cpp2::PropertyType type) {
  switch (type) {
    case nebula::meta::v2::cpp2::PropertyType::BOOL:
      return nebula::cpp2::PropertyType::BOOL;
    case nebula::meta::v2::cpp2::PropertyType::INT64:
      return nebula::cpp2::PropertyType::INT64;
    case nebula::meta::v2::cpp2::PropertyType::VID:
      return nebula::cpp2::PropertyType::VID;
    case nebula::meta::v2::cpp2::PropertyType::FLOAT:
      return nebula::cpp2::PropertyType::FLOAT;
    case nebula::meta::v2::cpp2::PropertyType::DOUBLE:
      return nebula::cpp2::PropertyType::DOUBLE;
    case nebula::meta::v2::cpp2::PropertyType::STRING:
      return nebula::cpp2::PropertyType::STRING;
    case nebula::meta::v2::cpp2::PropertyType::FIXED_STRING:
      return nebula::cpp2::PropertyType::FIXED_STRING;
    case nebula::meta::v2::cpp2::PropertyType::INT8:
      return nebula::cpp2::PropertyType::INT8;
    case nebula::meta::v2::cpp2::PropertyType::INT16:
      return nebula::cpp2::PropertyType::INT16;
    case nebula::meta::v2::cpp2::PropertyType::INT32:
      return nebula::cpp2::PropertyType::INT32;
    case nebula::meta::v2::cpp2::PropertyType::TIMESTAMP:
      return nebula::cpp2::PropertyType::TIMESTAMP;
    case nebula::meta::v2::cpp2::PropertyType::DATE:
      return nebula::cpp2::PropertyType::DATE;
    case nebula::meta::v2::cpp2::PropertyType::DATETIME:
      return nebula::cpp2::PropertyType::DATETIME;
    case nebula::meta::v2::cpp2::PropertyType::TIME:
      return nebula::cpp2::PropertyType::TIME;
    case nebula::meta::v2::cpp2::PropertyType::GEOGRAPHY:
      return nebula::cpp2::PropertyType::GEOGRAPHY;
    default:
      return nebula::cpp2::PropertyType::UNKNOWN;
  }
}

nebula::meta::cpp2::GeoShape MetaDataUpgrade::convertToGeoShape(
    nebula::meta::v2::cpp2::GeoShape shape) {
  switch (shape) {
    case nebula::meta::v2::cpp2::GeoShape::ANY:
      return nebula::meta::cpp2::GeoShape::ANY;
    case nebula::meta::v2::cpp2::GeoShape::POINT:
      return nebula::meta::cpp2::GeoShape::POINT;
    case nebula::meta::v2::cpp2::GeoShape::LINESTRING:
      return nebula::meta::cpp2::GeoShape::LINESTRING;
    case nebula::meta::v2::cpp2::GeoShape::POLYGON:
      return nebula::meta::cpp2::GeoShape::POLYGON;
    default:
      LOG(FATAL) << "Unimplemented";
  }
}

void MetaDataUpgrade::printSpacesV2(const folly::StringPiece &val) {
  auto oldProps = meta::v2::MetaServiceUtilsV2::parseSpace(val);
  LOG(INFO) << "Space name: " << oldProps.get_space_name();
  LOG(INFO) << "Partition num: " << oldProps.get_partition_num();
  LOG(INFO) << "Replica factor: " << oldProps.get_replica_factor();
  LOG(INFO) << "Charset name: " << oldProps.get_charset_name();
  LOG(INFO) << "Collate name: " << oldProps.get_collate_name();
  if (oldProps.group_name_ref().has_value()) {
    LOG(INFO) << "Group name: " << *oldProps.group_name_ref();
  }
}

Status MetaDataUpgrade::saveMachineAndZone(std::vector<kvstore::KV> data) {
  NG_LOG_AND_RETURN_IF_ERROR(put(data));
  return Status::OK();
}

}  // namespace meta
}  // namespace nebula
