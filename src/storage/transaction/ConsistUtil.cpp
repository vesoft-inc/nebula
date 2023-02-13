/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ConsistUtil.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "common/utils/NebulaKeyUtils.h"
namespace nebula {
namespace storage {
std::string ConsistUtil::primeTable(PartitionID partId) {
  auto item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kPrime);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}

std::string ConsistUtil::doublePrimeTable(PartitionID partId) {
  auto item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kDoublePrime);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}

std::string ConsistUtil::primePrefix(PartitionID partId) {
  return primeTable(partId) + NebulaKeyUtils::edgePrefix(partId);
}

std::string ConsistUtil::doublePrimePrefix(PartitionID partId) {
  return doublePrimeTable(partId) + NebulaKeyUtils::edgePrefix(partId);
}

std::string ConsistUtil::primeKey(size_t vIdLen, PartitionID partId, const cpp2::EdgeKey& edgeKey) {
  return primeTable(partId) + NebulaKeyUtils::edgeKey(vIdLen,
                                                      partId,
                                                      edgeKey.get_src().getStr(),
                                                      edgeKey.get_edge_type(),
                                                      edgeKey.get_ranking(),
                                                      edgeKey.get_dst().getStr());
}

folly::StringPiece ConsistUtil::edgeKeyFromPrime(const folly::StringPiece& key) {
  return folly::StringPiece(key.begin() + sizeof(PartitionID), key.end());
}

folly::StringPiece ConsistUtil::edgeKeyFromDoublePrime(const folly::StringPiece& key) {
  return folly::StringPiece(key.begin() + sizeof(PartitionID), key.end());
}

std::string ConsistUtil::doublePrime(size_t vIdLen, PartitionID partId, const cpp2::EdgeKey& key) {
  return doublePrimeTable(partId) + NebulaKeyUtils::edgeKey(vIdLen,
                                                            partId,
                                                            key.get_src().getStr(),
                                                            key.get_edge_type(),
                                                            key.get_ranking(),
                                                            key.get_dst().getStr());
}

RequestType ConsistUtil::parseType(folly::StringPiece val) {
  char identifier = val.str().back();
  switch (identifier) {
    case 'u':
      return RequestType::UPDATE;
    case 'a':
      return RequestType::INSERT;
    case 'd':
      return RequestType::DELETE;
    default:
      LOG(FATAL) << "should not happen, identifier is " << identifier;
      return RequestType::UNKNOWN;
  }
}

cpp2::UpdateEdgeRequest ConsistUtil::parseUpdateRequest(folly::StringPiece val) {
  cpp2::UpdateEdgeRequest req;
  apache::thrift::CompactSerializer::deserialize(val, req);
  return req;
}

cpp2::AddEdgesRequest ConsistUtil::parseAddRequest(folly::StringPiece val) {
  cpp2::AddEdgesRequest req;
  apache::thrift::CompactSerializer::deserialize(val, req);
  return req;
}

std::string ConsistUtil::strUUID() {
  static boost::uuids::random_generator gen;
  return boost::uuids::to_string(gen());
}

std::string ConsistUtil::ConsistUtil::edgeKey(size_t vIdLen,
                                              PartitionID partId,
                                              const cpp2::EdgeKey& key) {
  return NebulaKeyUtils::edgeKey(vIdLen,
                                 partId,
                                 key.get_src().getStr(),
                                 *key.edge_type_ref(),
                                 *key.ranking_ref(),
                                 (*key.dst_ref()).getStr());
}

cpp2::AddEdgesRequest ConsistUtil::toAddEdgesRequest(const cpp2::ChainAddEdgesRequest& req) {
  cpp2::AddEdgesRequest ret;
  ret.space_id_ref() = req.get_space_id();
  ret.parts_ref() = req.get_parts();
  ret.prop_names_ref() = req.get_prop_names();
  ret.if_not_exists_ref() = req.get_if_not_exists();
  return ret;
}

cpp2::EdgeKey ConsistUtil::reverseEdgeKey(const cpp2::EdgeKey& edgeKey) {
  cpp2::EdgeKey reversedKey(edgeKey);
  std::swap(*reversedKey.src_ref(), *reversedKey.dst_ref());
  *reversedKey.edge_type_ref() = 0 - edgeKey.get_edge_type();
  return reversedKey;
}

void ConsistUtil::reverseEdgeKeyInplace(cpp2::EdgeKey& edgeKey) {
  cpp2::EdgeKey reversedKey(edgeKey);
  std::swap(*edgeKey.src_ref(), *edgeKey.dst_ref());
  *edgeKey.edge_type_ref() = 0 - edgeKey.get_edge_type();
}

int64_t ConsistUtil::toInt(const ::nebula::Value& val) {
  auto str = val.toString();
  if (str.size() < 3) {
    return 0;
  }
  return *reinterpret_cast<int64*>(const_cast<char*>(str.data() + 1));
}

std::string ConsistUtil::readableKey(size_t vidLen, bool isIntVid, const std::string& rawKey) {
  auto src = NebulaKeyUtils::getSrcId(vidLen, rawKey);
  auto dst = NebulaKeyUtils::getDstId(vidLen, rawKey);
  auto rank = NebulaKeyUtils::getRank(vidLen, rawKey);
  std::stringstream ss;
  ss << std::boolalpha << "isIntVid=" << isIntVid << ", ";
  if (isIntVid) {
    ss << *reinterpret_cast<int64*>(const_cast<char*>(src.begin())) << "--"
       << *reinterpret_cast<int64*>(const_cast<char*>(dst.begin()));
  } else {
    ss << src.str() << "--" << dst.str();
  }
  ss << "@" << rank;
  return ss.str();
}

std::vector<std::string> ConsistUtil::toStrKeys(const cpp2::DeleteEdgesRequest& req, int vIdLen) {
  std::vector<std::string> ret;
  for (auto& edgesOfPart : req.get_parts()) {
    auto partId = edgesOfPart.first;
    for (auto& key : edgesOfPart.second) {
      ret.emplace_back(ConsistUtil::edgeKey(vIdLen, partId, key));
    }
  }
  return ret;
}

::nebula::cpp2::ErrorCode ConsistUtil::getErrorCode(const cpp2::ExecResponse& resp) {
  auto ret = ::nebula::cpp2::ErrorCode::SUCCEEDED;
  auto& respComn = resp.get_result();
  for (auto& part : respComn.get_failed_parts()) {
    ret = part.code;
  }
  return ret;
}

cpp2::DeleteEdgesRequest DeleteEdgesRequestHelper::toDeleteEdgesRequest(
    const cpp2::ChainDeleteEdgesRequest& req) {
  cpp2::DeleteEdgesRequest ret;
  ret.space_id_ref() = req.get_space_id();
  ret.parts_ref() = req.get_parts();
  return ret;
}

cpp2::DeleteEdgesRequest DeleteEdgesRequestHelper::parseDeleteEdgesRequest(const std::string& val) {
  cpp2::DeleteEdgesRequest req;
  apache::thrift::CompactSerializer::deserialize(val, req);
  return req;
}

std::string DeleteEdgesRequestHelper::explain(const cpp2::DeleteEdgesRequest& req, bool isIntVid) {
  std::stringstream oss;
  for (auto& partOfKeys : req.get_parts()) {
    for (auto& key : partOfKeys.second) {
      if (isIntVid) {
        oss << ConsistUtil::toInt(key.get_src()) << "->" << ConsistUtil::toInt(key.get_dst()) << "@"
            << key.get_ranking() << ", ";
      }
    }
  }
  return oss.str();
}

}  // namespace storage
}  // namespace nebula
