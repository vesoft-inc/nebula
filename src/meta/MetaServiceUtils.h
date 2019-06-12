/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAUTILS_H_
#define META_METAUTILS_H_

#include "base/Base.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

enum class EntryType : int8_t {
    SPACE = 0x01,
    TAG   = 0x02,
    EDGE  = 0x03,
};

class MetaServiceUtils final {
public:
    MetaServiceUtils() = delete;

    static std::string spaceKey(GraphSpaceID spaceId);

    static std::string spaceVal(const cpp2::SpaceProperties &properties);

    static cpp2::SpaceProperties parseSpace(folly::StringPiece rawData);

    static const std::string& spacePrefix();

    static GraphSpaceID spaceId(folly::StringPiece rawKey);

    static std::string spaceName(folly::StringPiece rawVal);

    static std::string partKey(GraphSpaceID spaceId, PartitionID partId);

    static std::string partVal(const std::vector<nebula::cpp2::HostAddr>& hosts);

    static const std::string& partPrefix();

    static std::string partPrefix(GraphSpaceID spaceId);

    static std::vector<nebula::cpp2::HostAddr> parsePartVal(folly::StringPiece val);

    static std::string hostKey(IPv4 ip, Port port);

    static std::string hostValOnline();

    static std::string hostValOffline();

    static const std::string& hostPrefix();

    static nebula::cpp2::HostAddr parseHostKey(folly::StringPiece key);

    static std::string schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType);

    static std::string schemaEdgesPrefix(GraphSpaceID spaceId);

    static std::string schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer version);

    static std::string schemaEdgeVal(const std::string& name, nebula::cpp2::Schema schema);

    static SchemaVer parseEdgeVersion(folly::StringPiece key);

    static std::string schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version);

    static SchemaVer parseTagVersion(folly::StringPiece key);

    static std::string schemaTagPrefix(GraphSpaceID spaceId, TagID tagId);

    static std::string schemaTagsPrefix(GraphSpaceID spaceId);

    static std::string schemaTagVal(const std::string& name, nebula::cpp2::Schema schema);

    static nebula::cpp2::Schema parseSchema(folly::StringPiece rawData);

    static std::string indexSpaceKey(const std::string& name);

    static std::string indexTagKey(GraphSpaceID spaceId, const std::string& name);

    static std::string indexEdgeKey(GraphSpaceID spaceId, const std::string& name);

    static std::string assembleSegmentKey(const std::string& segment, const std::string& key);

    static cpp2::ErrorCode alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                           const nebula::cpp2::ColumnDef col,
                                           const cpp2::AlterSchemaOp op);
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METAUTILS_H_

