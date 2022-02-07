/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_UPGRADE_METADATAUPGRADE_H
#define META_UPGRADE_METADATAUPGRADE_H
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include "meta/upgrade/v2/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class MetaDataUpgrade final {
 public:
  explicit MetaDataUpgrade(kvstore::KVEngine *engine) : engine_(engine) {}

  ~MetaDataUpgrade() = default;

  Status rewriteSpacesV2ToV3(const folly::StringPiece &key, const folly::StringPiece &val);

  Status deleteKeyVal(const folly::StringPiece &key);

  Status saveMachineAndZone(std::vector<kvstore::KV> data);

  void printSpacesV2(const folly::StringPiece &val);

 private:
  Status put(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto ret = engine_->put(key.str(), val.str());
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return Status::Error("Put data failed");
    }
    return Status::OK();
  }

  Status put(std::vector<kvstore::KV> data) {
    auto ret = engine_->multiPut(data);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return Status::Error("Put data failed");
    }
    return Status::OK();
  }

  Status remove(const folly::StringPiece &key) {
    auto ret = engine_->remove(key.str());
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return Status::Error("Remove data failed");
    }
    return Status::OK();
  }

  nebula::cpp2::PropertyType convertToPropertyType(nebula::meta::v2::cpp2::PropertyType type);

  nebula::meta::cpp2::GeoShape convertToGeoShape(nebula::meta::v2::cpp2::GeoShape shape);

 private:
  kvstore::KVEngine *engine_ = nullptr;
};

}  // namespace meta
}  // namespace nebula
#endif
