// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/util/Utils.h"

#include "interface/gen-cpp2/storage_types.h"

namespace nebula::graph::util {

folly::dynamic getStorageDetail(const std::map<std::string, int32_t>& profileDetail) {
  folly::dynamic profileData = folly::dynamic::object();
  for (auto& p : profileDetail) {
    profileData.insert(p.first, folly::sformat("{}(us)", p.second));
  }
  return profileData;
}

folly::dynamic collectRespProfileData(const storage::cpp2::ResponseCommon& resp,
                                      const std::tuple<HostAddr, int32_t, int32_t>& info,
                                      size_t numVertices) {
  folly::dynamic stat = folly::dynamic::object();
  stat.insert("host", std::get<0>(info).toRawString());
  stat.insert("exec", folly::sformat("{}(us)", std::get<1>(info)));
  stat.insert("total", folly::sformat("{}(us)", std::get<2>(info)));
  if (numVertices > 0) {
    stat.insert("vertices", numVertices);
  }
  if (resp.latency_detail_us_ref().has_value()) {
    stat.insert("storage_detail", getStorageDetail(*resp.get_latency_detail_us()));
  }
  return stat;
}

}  // namespace nebula::graph::util
