/* Copyright (c) 2018 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_H_
#define TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "common/interface/gen-cpp2/meta_types.h"

#include "meta/upgradeData/oldThrift/gen-cpp2/old_meta_types.h"
#include "kvstore/Common.h"


namespace nebula {
namespace oldmeta {

const std::string kSpacesTable         = "__spaces__";         // NOLINT
const std::string kPartsTable          = "__parts__";          // NOLINT
const std::string kHostsTable          = "__hosts__";          // NOLINT
const std::string kTagsTable           = "__tags__";           // NOLINT
const std::string kEdgesTable          = "__edges__";          // NOLINT
const std::string kIndexesTable        = "__indexes__";        // NOLINT
const std::string kIndexTable          = "__index__";          // NOLINT
const std::string kIndexStatusTable    = "__index_status__";   // NOLINT
const std::string kUsersTable          = "__users__";          // NOLINT
const std::string kRolesTable          = "__roles__";          // NOLINT
const std::string kConfigsTable        = "__configs__";        // NOLINT
const std::string kDefaultTable        = "__default__";        // NOLINT
const std::string kSnapshotsTable      = "__snapshots__";      // NOLINT
const std::string kLastUpdateTimeTable = "__last_update_time__"; // NOLINT
const std::string kLeadersTable        = "__leaders__";          // NOLINT
const std::string kCurrJob             = "__job_mgr____id"; // NOLINT
const std::string kJob                 = "__job_mgr_";         // NOLINT
const std::string kJobArchive          = "__job_mgr_archive_"; // NOLINT

using ConfigName = std::pair<cpp2::ConfigModule, std::string>;

class MetaServiceUtilsV1 final {
public:
    MetaServiceUtilsV1() = delete;

    static std::string spaceVal(const cpp2::SpaceProperties &properties);

    static cpp2::SpaceProperties parseSpace(folly::StringPiece rawData);

    static GraphSpaceID parsePartKeySpaceId(folly::StringPiece key);

    static PartitionID parsePartKeyPartId(folly::StringPiece key);

    static std::vector<cpp2::HostAddr> parsePartVal(folly::StringPiece val);

    static cpp2::HostAddr parseHostKey(folly::StringPiece key);

    static cpp2::HostAddr parseLeaderKey(folly::StringPiece key);

    static cpp2::Schema parseSchema(folly::StringPiece rawData);

    static cpp2::IndexItem parseIndex(const folly::StringPiece& rawData);

    static ConfigName parseConfigKey(folly::StringPiece rawData);

    static cpp2::ConfigItem parseConfigValue(folly::StringPiece rawData);

    static int32_t parseJobId(const folly::StringPiece& rawKey);

    static std::tuple<std::string,
                      std::vector<std::string>,
                      nebula::meta::cpp2::JobStatus,
                      int64_t,
                      int64_t>
    parseJobDesc(const folly::StringPiece& rawVal);
};

}  // namespace oldmeta
}  // namespace nebula
#endif  // TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_H_

