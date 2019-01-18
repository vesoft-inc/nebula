/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/FileBasedPartManager.h"
#include <folly/Hash.h>
#include <folly/String.h>
#include "network/NetworkUtils.h"
#include "base/Configuration.h"
#include "meta/SchemaManager.h"

DEFINE_string(partition_conf_file, "",
              "The full path of the partition allocation file");


namespace nebula {
namespace meta {

using nebula::network::NetworkUtils;

// static
std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> FileBasedPartManager::init() {
    DCHECK(!FLAGS_partition_conf_file.empty());

    Configuration conf;
    CHECK(conf.parseFromFile(FLAGS_partition_conf_file).ok());

    // Retrieve all spaces
    std::unordered_map<std::string, GraphSpaceID> spaces;
    CHECK(conf.forEachKey([&spaces] (const std::string& spaceName) {
        spaces.emplace(spaceName, SchemaManager::toGraphSpaceID(spaceName));
    }).ok());

    // Retrieve partition allocation for each GraphSpace
    std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> pms;
    for (auto& s : spaces) {
        Configuration spaceConf;
        CHECK(conf.fetchAsSubConf(s.first.c_str(), spaceConf).ok());
        std::shared_ptr<FileBasedPartManager> pm(new FileBasedPartManager());
        CHECK(spaceConf.forEachItem([&pm] (const std::string& hostStr,
                                           const folly::dynamic& parts) {
            CHECK(parts.isArray());
            auto host = NetworkUtils::toHostAddr(hostStr);
            pm->hosts_.emplace_back(host);
            for (auto& entry : parts) {
                CHECK(entry.isInt());
                pm->partHostMap_[entry.getInt()].emplace_back(host);
                pm->hostPartMap_[host].emplace_back(entry.getInt());
            }
        }).ok());
        pms.emplace(s.second, pm);
    }

    return pms;
}

}  // namespace meta
}  // namespace nebula

