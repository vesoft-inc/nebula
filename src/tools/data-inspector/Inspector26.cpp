/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "tools/data-inspector/Inspector26.h"

#include "common/utils/Types.h"
#include "common/thrift/ThriftTypes.h"

DECLARE_int32(num_samples);
DECLARE_int32(num_leading_spaces);
DECLARE_int64(num_entries_to_dump);


namespace nebula {

std::string Inspector26::parseVertexKey(rocksdb::Slice key) const {
  std::string buf;
  buf.append(FLAGS_num_leading_spaces, ' ');

  PartitionID partId = 0;
  memcpy(reinterpret_cast<char*>(&partId),
         key.data() + 1,
         sizeof(PartitionID) - 1);
  buf.append(folly::stringPrintf("[PartId:%d]", partId));

  TagID tagId = 0;
  memcpy(reinterpret_cast<char*>(&tagId),
         key.data() + key.size() - sizeof(TagID),
         sizeof(TagID));
  buf.append(folly::stringPrintf(" [TagId:%d]", tagId));

  size_t vidLen = key.size() - sizeof(PartitionID) - sizeof(TagID);
  buf.append(
    folly::stringPrintf(" [VertexId(%ld):%s]",
                        vidLen,
                        decodeHexStr(key.data() + sizeof(PartitionID), vidLen).c_str()));
  return buf;
}


std::string Inspector26::parseEdgeKey(rocksdb::Slice key) const {
  std::string buf;
  buf.append(FLAGS_num_leading_spaces, ' ');

  PartitionID partId = 0;
  memcpy(reinterpret_cast<char*>(&partId),
         key.data() + 1,
         sizeof(PartitionID) - 1);
  buf.append(folly::stringPrintf("[PartId:%d]", partId));

  size_t idLen = (key.size() - sizeof(PartitionID) - sizeof(char) /*edge ver placeholder */
                             - sizeof(EdgeRanking) - sizeof(EdgeType)) / 2;

  // Edge type
  EdgeType et = 0;
  memcpy(reinterpret_cast<char*>(&et),
         key.data() + sizeof(PartitionID) + idLen,
         sizeof(EdgeType));
  buf.append(folly::stringPrintf(" [EdgeType:%d]", et));

  // Rank
  EdgeRanking rank = 0;
  memcpy(reinterpret_cast<char*>(&rank),
         key.data() + sizeof(PartitionID) + idLen + sizeof(EdgeType),
         sizeof(EdgeRanking));
  buf.append(folly::stringPrintf(" [Rank:%ld]\n", rank));

  // SrcId
  buf.append(FLAGS_num_leading_spaces, ' ');
  buf.append(folly::stringPrintf("[srcId(%ld):%s]\n",
                                 idLen,
                                 decodeHexStr(key.data() + sizeof(PartitionID), idLen).c_str()));

  // DstId
  buf.append(FLAGS_num_leading_spaces, ' ');
  buf.append(folly::stringPrintf(
    "[dstId(%ld):%s]",
    idLen,
    decodeHexStr(key.data() + key.size() - sizeof(char) - idLen, idLen).c_str()));

  return buf;
}


std::string Inspector26::parseSystemKey(rocksdb::Slice key) const {
  std::string buf;
  buf.append(FLAGS_num_leading_spaces, ' ');

  PartitionID partId = 0;
  memcpy(reinterpret_cast<char*>(&partId),
         key.data() + 1,
         sizeof(PartitionID) - 1);
  buf.append(folly::stringPrintf("[PartId:%d]", partId));

  NebulaSystemKeyType type;
  memcpy(reinterpret_cast<char*>(&type),
         key.data() + sizeof(PartitionID),
         sizeof(NebulaSystemKeyType));
  if (type == NebulaSystemKeyType::kSystemCommit) {
    buf.append(" [Type:Commit]");
  } else {
    // Part
    buf.append(" [Type:Partition]");
  }

  return buf;
}


std::string Inspector26::parseKVKey(rocksdb::Slice key) const {
  std::string buf;
  buf.append(FLAGS_num_leading_spaces, ' ');

  // Partition
  PartitionID partId = 0;
  memcpy(reinterpret_cast<char*>(&partId),
         key.data() + 1,
         sizeof(PartitionID) - 1);
  buf.append(folly::stringPrintf("[PartId:%d]", partId));

  // Key name
  buf.append(folly::stringPrintf(
    " [name(%ld):%s]",
    key.size() - sizeof(PartitionID),
    decodeHexStr(key.data() + sizeof(PartitionID), key.size() - sizeof(PartitionID)).c_str()));

  return buf;
}


std::string Inspector26::parseKey(rocksdb::Slice key) const {
  std::string buf;

  switch (*key.data()) {
    case '\01':
      buf.append(folly::stringPrintf("Vertex(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      buf.append("\n");
      buf.append(parseVertexKey(key));
      break;
    case '\02':
      buf.append(folly::stringPrintf("Edge(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      buf.append("\n");
      buf.append(parseEdgeKey(key));
      break;
    case '\03':
      buf.append(folly::stringPrintf("Index(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      break;
    case '\04':
      buf.append(folly::stringPrintf("System(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      buf.append("\n");
      buf.append(parseSystemKey(key));
      break;
    case '\05':
      buf.append(folly::stringPrintf("Operation(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      break;
    case '\06':
      buf.append(folly::stringPrintf("KV(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      buf.append("\n");
      buf.append(parseKVKey(key));
      break;
    default:
      buf.append(folly::stringPrintf("!!Unknown(%ld) => ", key.size()).c_str());
      buf.append(key.ToString(true));
      buf.append("\n");
      buf.append(FLAGS_num_leading_spaces, ' ');
      buf.append(decodeHexStr(key.data(), key.size()).c_str());
      break;
  }

  return buf;
}


void Inspector26::info() const {
  rocksdb::ReadOptions ro;
  auto* it = db_->NewIterator(ro);

  // First key
  it->SeekToFirst();
  if (it->Valid()) {
    fprintf(stdout, "First key: %s\n", parseKey(it->key()).c_str());
  }
  // Last key
  it->SeekToLast();
  if (it->Valid()) {
    fprintf(stdout, "Last key: %s\n", parseKey(it->key()).c_str());
  }
}


void Inspector26::stats() const {
  rocksdb::ReadOptions ro;
  auto* it = db_->NewIterator(ro);
  it->SeekToFirst();

  size_t count[7];
  size_t remainingSamples[7];
  for (size_t i = 0; i < 7; i++) {
    count[i] = 0;
    remainingSamples[i] = FLAGS_num_samples;
  }

  while (it->Valid()) {
    int idx = *(it->key().data());
    if (idx < 1 || idx > 6) {
      // Unknown key
      idx = 0;
    }

    count[idx]++;

    if (remainingSamples[idx] > 0) {
      remainingSamples[idx]--;
      fprintf(stdout, "%s\n", parseKey(it->key()).c_str());
    }

    it->Next();
  }

  fprintf(stdout, "\nTotal number of keys: %ld\n",
          count[0] + count[1] + count[2] + count[3] + count[4] + count[5] + count[6]);
  fprintf(stdout, "  # vertices: %ld\n", count[1]);
  fprintf(stdout, "  # edges: %ld\n", count[2]);
  fprintf(stdout, "  # indices: %ld\n", count[3]);
  fprintf(stdout, "  # system keys: %ld\n", count[4]);
  fprintf(stdout, "  # operation keys: %ld\n", count[5]);
  fprintf(stdout, "  # KVs: %ld\n", count[6]);
  fprintf(stdout, "  # !!unknown: %ld\n", count[0]);
}


void Inspector26::dump() const {
  rocksdb::ReadOptions ro;
  auto* it = db_->NewIterator(ro);

  std::string prefix = getPrefix();
  if (prefix.empty()) {
    fprintf(stdout, "No prefix provided. Will do a full scan\n\n");
    it->SeekToFirst();
  } else {
    fprintf(stdout,
            "Prefix(%ld) = %s\n\n",
            prefix.size(),
            decodeHexStr(prefix.data(), prefix.size()).c_str());
    it->Seek(prefix);
  }

  ssize_t numEntriesDumped = 1;
  ssize_t numEntriesIgnored = 0;
  while (it->Valid() && (prefix.empty() || it->key().starts_with(prefix))) {
    if (FLAGS_num_entries_to_dump == 0 || numEntriesDumped <= FLAGS_num_entries_to_dump) {
      fprintf(stdout, "%s\n", parseKey(it->key()).c_str());
      numEntriesDumped++;
    } else {
      numEntriesIgnored++;
    }
    it->Next();
  }

  if (numEntriesIgnored > 0) {
    fprintf(stdout, ">>>>> There are %ld more entries <<<<<\n", numEntriesIgnored);
  }
}

}  // namespace nebula


