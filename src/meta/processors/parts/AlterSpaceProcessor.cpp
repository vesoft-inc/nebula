/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/AlterSpaceProcessor.h"

namespace nebula {
namespace meta {
void AlterSpaceProcessor::process(const cpp2::AlterSpaceReq& req) {
  const std::vector<std::string>& zones = req.get_paras();
  const std::string& spaceName = req.get_space_name();
  cpp2::AlterSpaceOp op = req.get_op();
  switch (op) {
    case cpp2::AlterSpaceOp::ADD_ZONE: {
      nebula::cpp2::ErrorCode ret = addZones(spaceName, zones);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(ret);
        onFinished();
        return;
      }
      break;
    }
    default:
      break;
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

nebula::cpp2::ErrorCode AlterSpaceProcessor::addZones(const std::string& spaceName,
                                                      const std::vector<std::string>& zones) {
  auto spaceRet = getSpaceId(spaceName);
  if (!nebula::ok(spaceRet)) {
    auto retCode = nebula::error(spaceRet);
    return retCode;
  }
  auto spaceId = nebula::value(spaceRet);
  std::string spaceKey = MetaKeyUtils::spaceKey(spaceId);
  std::string spaceVal;
  nebula::cpp2::ErrorCode retCode =
      kvstore_->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceVal);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return retCode;
  }
  meta::cpp2::SpaceDesc properties = MetaKeyUtils::parseSpace(spaceVal);
  const std::vector<std::string>& curZones = properties.get_zone_names();
  std::set<std::string> zm;
  for (const std::string& z : curZones) {
    zm.insert(z);
  }
  std::vector<std::string> newZones = curZones;
  newZones.reserve(curZones.size() + zones.size());
  for (const std::string& z : zones) {
    std::string zoneKey = MetaKeyUtils::zoneKey(z);
    std::string zoneVal;
    nebula::cpp2::ErrorCode zoneRet =
        kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneVal);
    if (zoneRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return zoneRet == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND
                 ? nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND
                 : zoneRet;
    }
    if (zm.count(z)) {
      return nebula::cpp2::ErrorCode::E_CONFLICT;
    }
    newZones.emplace_back(z);
  }
  properties.set_zone_names(newZones);
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::spaceKey(spaceId), MetaKeyUtils::spaceVal(properties));
  folly::Baton<true, std::atomic> baton;
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiPut(kDefaultSpaceId,
                          kDefaultPartId,
                          std::move(data),
                          [&ret, &baton](nebula::cpp2::ErrorCode code) {
                            if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                              ret = code;
                              LOG(INFO) << "Put data error on meta server";
                            }
                            baton.post();
                          });
  baton.wait();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
