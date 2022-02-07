/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "tools/data-inspector/Inspector.h"
#include "common/fs/FileUtils.h"
#include "common/thrift/ThriftTypes.h"
#include "tools/data-inspector/Inspector26.h"

DEFINE_string(db_path, "./", "The path to a rocksdb instance");
DEFINE_string(prefix, "", "The prefix information for lookup operation");
DEFINE_int32(num_samples, 0,
             "The number of sample keys to be displayed for each type");
DEFINE_int32(num_leading_spaces, 4,
             "The number of leading spaces when displaying detail lines");
DEFINE_int64(num_entries_to_dump, 10,
             "The number of entries to be displayed when looknig up by prefix");

namespace nebula {

Inspector::~Inspector() {
  if (db_ != nullptr) {
    db_->Close();
    db_ = nullptr;
  }
}


// static
rocksdb::DB* Inspector::openDB() {
  if (!fs::FileUtils::exist(FLAGS_db_path)) {
    LOG(ERROR) << "Db path '" << FLAGS_db_path << "' does not exist";
    return nullptr;
  }

  rocksdb::Options options;
  rocksdb::DB* dbPtr;
  auto status = rocksdb::DB::OpenForReadOnly(options, FLAGS_db_path, &dbPtr);
  if (!status.ok()) {
    LOG(ERROR) << "Unable to open database '" << FLAGS_db_path << "' for reading: "
               << status.ToString();
    return nullptr;
  }

  return dbPtr;
}


// static
std::unique_ptr<Inspector> Inspector::getInspector() {
  rocksdb::DB* db = openDB();
  if (db != nullptr) {
    // TODO(sye) should generate an inspector based on the data version
    return std::make_unique<Inspector26>(db);
  } else {
    return std::unique_ptr<Inspector>();
  }
}


std::string Inspector::decodeHexStr(const char* start, size_t len) const {
  std::string buf;
  for (size_t i = 0; i < len; i++) {
    unsigned char ch = *(start + i);
    buf.append(folly::stringPrintf("%02X", ch));
    if (*(start + i) >= 32) {
      buf.append(folly::stringPrintf("(%c)", *(start + i)));
    }
    buf.append(" ");
  }
  return buf;
}


std::string Inspector::processPrefixInt8(const std::string& valStr, int8_t& val) const {
  try {
    val = folly::to<int8_t>(valStr);
    std::string buf;
    buf.append(reinterpret_cast<char*>(&val), sizeof(int8_t));
    return buf;
  } catch (const std::exception& ex) {
    LOG(FATAL) << "Invalid int8_t value \"" << valStr << "\"";
  }
}


std::string Inspector::processPrefixInt32(const std::string& valStr, int32_t& val) const {
  try {
    val = folly::to<int32_t>(valStr);
    std::string buf;
    buf.append(reinterpret_cast<char*>(&val), sizeof(int32_t));
    return buf;
  } catch (const std::exception& ex) {
    LOG(FATAL) << "Invalid int32_t value \"" << valStr << "\"";
  }
}


std::string Inspector::processPrefixInt64(const std::string& valStr, int64_t& val) const {
  try {
    val = folly::to<int64_t>(valStr);
    std::string buf;
    buf.append(reinterpret_cast<char*>(&val), sizeof(int64_t));
    return buf;
  } catch (const std::exception& ex) {
    LOG(FATAL) << "Invalid int64_t value \"" << valStr << "\"";
  }
}


std::string Inspector::processPrefixHex(const std::string& valStr) const {
  if (valStr.size() / 2 * 2 != valStr.size()) {
    LOG(FATAL) << "The length  of the HEX string must be an even number";
  }

  static const std::unordered_map<char, char> dict = {
    {'0', 0x00}, {'1', 0x01}, {'2', 0x02}, {'3', 0x03},
    {'4', 0x04}, {'5', 0x05}, {'6', 0x06}, {'7', 0x07},
    {'8', 0x08}, {'9', 0x09}, {'A', 0x0A}, {'B', 0x0B},
    {'C', 0x0C}, {'D', 0x0D}, {'E', 0x0E}, {'F', 0x0F}
  };

  std::string buf;
  // Skip the leading "\\x"
  for (size_t i = 2; i < valStr.size(); i += 2) {
    char ch;
    auto it = dict.find(std::toupper(valStr[i]));
    if (it == dict.end()) {
      LOG(FATAL) << "The HEX string contains invalid character '" << valStr[i] << "'";
    }
    ch = it->second << 4;

    it = dict.find(std::toupper(valStr[i + 1]));
    if (it == dict.end()) {
      LOG(FATAL) << "The HEX string contains invalid character '" << valStr[i + 1] << "'";
    }
    ch |= it->second;

    buf.append(1, ch);
  }

  return buf;
}


std::unordered_map<std::string, std::string> Inspector::preparePrefix() const {
  std::unordered_map<std::string, std::string> processedParts;

  if (FLAGS_prefix.empty()) {
    return processedParts;
  }

  std::vector<std::string> parts;
  folly::split(";", FLAGS_prefix, parts);

  // Now let's pre-process all prefix parts
  for (auto& part : parts) {
    std::vector<std::string> kv;
    folly::split(":", part, kv);
    if (kv.size() != 2) {
      LOG(FATAL) << "Invalid prefix: \"" << part << "\"";
    }

    std::string buf;
    if (kv[0] == "type") {
      // Key type: int8
      int8_t type;
      buf = processPrefixInt8(kv[1], type);
      if (type < 1 || type > 6) {
        LOG(FATAL) << "Unknown key type " << type;
      }
    } else if (kv[0] == "part" || kv[0] == "tag" || kv[0] == "edge" || kv[0] == "idlen") {
      // Partition, tag, edgeType, idLen: int32
      int32_t val;
      buf = processPrefixInt32(kv[1], val);
      if (kv[0] == "part") {
        if (val < 1 || val > 0x00FFFFFF) {
          LOG(FATAL) << "Invalid partition id " << val;
        }
      }
    } else if (kv[0] == "rank") {
      // Rank on th edge: int64
      int64_t val;
      buf = processPrefixInt64(kv[1], val);
    } else if (kv[0] == "vid" || kv[0] == "dst") {
      // "vid" can be used as vertexId or srcId: Normal string or Hex string
      if (kv[1][0] == '\\' && (kv[1][1] == 'x' || kv[1][1] == 'X')) {
        // Hex string
        buf = processPrefixHex(kv[1]);
      } else {
        // Normal string
        buf = kv[1];
      }
    } else if (kv[0] == "hex") {
      // Hex string
      buf = processPrefixHex(kv[1]);
    } else {
      LOG(FATAL) << "Unknown prefix part \"" << kv[0] << "\"";
    }

    processedParts.emplace(std::make_pair(kv[0], buf));
  }

  return processedParts;
}


std::string Inspector::getPrefix() const {
  std::unordered_map<std::string, std::string> parts = preparePrefix();
  if (parts.empty()) {
    return "";
  }

  std::string prefix;

  auto it = parts.find("hex");
  if (it != parts.end()) {
    // The full prefix is given as a hex string
    LOG(INFO) << "A hex prefix string is given";
    return it->second;
  }

  it = parts.find("type");
  if (it == parts.end()) {
    // No type is given, will do full scan
    return prefix;
  }
  CHECK_EQ(it->second.size(), 1L);
  int8_t type = it->second[0];
  prefix.append(it->second);

  it = parts.find("part");
  if (it == parts.end()) {
    // No partition id
    LOG(INFO) << "No partition id is specified";
    return prefix;
  }
  CHECK_EQ(it->second.size(), 4L);
  prefix.append(it->second, 0, 3);

  int32_t idLen = 0;
  it = parts.find("idlen");
  if (it != parts.end()) {
    CHECK_EQ(it->second.size(), sizeof(int32_t));
    memcpy(reinterpret_cast<char*>(&idLen), it->second.data(), sizeof(int32_t));
  }

  switch (type) {
    case 0x01: {
      // Vertex
      LOG(INFO) << "Preparing the prefix for vertices";

      it = parts.find("vid");
      if (it == parts.end()) {
        return prefix;
      }
      int32_t vidSize = it->second.size();
      if (idLen == 0 || idLen >= vidSize) {
        prefix.append(it->second);
      } else {
        prefix.append(it->second.data(), idLen);
      }

      it = parts.find("tag");
      if (it == parts.end()) {
        return prefix;
      }
      CHECK_EQ(it->second.size(), sizeof(TagID));
      // Paddings for the vertex id
      if (idLen > vidSize) {
        prefix.append(idLen - vidSize, '\x00');
      }
      // Tag id
      prefix.append(it->second);
      break;
    }

    case 0x02: {
      // Edge
      LOG(INFO) << "Preparing the prefix for edges";

      // srcId
      it = parts.find("vid");
      if (it == parts.end()) {
        return prefix;
      }
      int32_t vidSize = it->second.size();
      if (idLen == 0 || idLen >= vidSize) {
        prefix.append(it->second);
      } else {
        prefix.append(it->second.data(), idLen);
      }

      // Edge type
      it = parts.find("edge");
      if (it == parts.end()) {
        return prefix;
      }
      CHECK_EQ(it->second.size(), sizeof(EdgeType));
      // Paddings for the source vertex id
      if (idLen > vidSize) {
        prefix.append(idLen - vidSize, '\x00');
      }
      prefix.append(it->second);

      // Rank
      it = parts.find("rank");
      if (it == parts.end()) {
        return prefix;
      }
      CHECK_EQ(it->second.size(), sizeof(EdgeRanking));
      prefix.append(it->second);

      // dstId
      it = parts.find("dst");
      if (it == parts.end()) {
        return prefix;
      }
      if (idLen == 0 || static_cast<size_t>(idLen) >= it->second.size()) {
        prefix.append(it->second);
      } else {
        prefix.append(it->second.data(), idLen);
      }
      break;
    }

    case 0x03: {
      // Index
      LOG(INFO) << "Preparing the prefix for indices";
    }

    case 0x04: {
      // System
      LOG(INFO) << "Preparing the prefix for system keys";
    }

    case 0x05: {
      // Operation
      LOG(INFO) << "Preparing the prefix for operations";
    }

    case 0x06: {
      // KV
      LOG(INFO) << "Preparing the prefix for general key-values";
    }

    default: {
      LOG(FATAL) << "Unknown type: " << static_cast<int32_t>(it->second[0]);
    }
  }

  return prefix;
}

}  // namespace nebula

