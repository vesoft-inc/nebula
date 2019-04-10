/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METAUTILS_H_
#define META_METAUTILS_H_

#include "base/Base.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace meta {

enum class EntryType : int8_t {
    SPACE = 0x01,
    TAG   = 0x02,
    EDGE  = 0x03,
};

class MetaUtils final {
public:
    MetaUtils() = delete;

    static std::string spaceKey(GraphSpaceID spaceId);

    // TODO(dangleptr) We should use one struct to represent space properties.
    static std::string spaceVal(int32_t partsNum, int32_t replicaFactor, const std::string& name);

    static const std::string& spacePrefix();

    static GraphSpaceID spaceId(folly::StringPiece rawKey);

    static folly::StringPiece spaceName(folly::StringPiece rawVal);

    static std::string partKey(GraphSpaceID spaceId, PartitionID partId);

    static std::string partVal(const std::vector<nebula::cpp2::HostAddr>& hosts);

    static std::string partPrefix(GraphSpaceID spaceId);

    static std::vector<nebula::cpp2::HostAddr> parsePartVal(folly::StringPiece val);

    static std::string hostKey(IPv4 ip, Port port);

    static std::string hostVal();

    static const std::string& hostPrefix();

    static nebula::cpp2::HostAddr parseHostKey(folly::StringPiece key);

    static std::string schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, int64_t version);

    static std::string schemaEdgeVal(nebula::cpp2::Schema schema);

    static std::string schemaTagKey(GraphSpaceID spaceId, TagID tagId, int64_t version);

    static std::string schemaTagsPrefix(GraphSpaceID spaceId);

    static std::string schemaTagVal(const std::string& name, nebula::cpp2::Schema schema);

    static nebula::cpp2::Schema parseSchema(folly::StringPiece rawData);

    static std::string indexKey(EntryType type, const std::string& name);
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METAUTILS_H_

