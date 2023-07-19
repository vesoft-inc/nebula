/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"

#include <random>

#include "fmt/printf.h"
#include "openssl/sha.h"
namespace nebula::plugin {

using namespace fmt::literals;  // NOLINT

ESQueryResult::Item::Item(const std::string& v, double sc) : vid(v), score(sc) {}
ESQueryResult::Item::Item(const std::string& s, const std::string& d, int64_t r, double sc)
    : src(s), dst(d), rank(r), score(sc) {}

void ESBulk::put(const std::string& indexName,
                 const std::string& vid,
                 const std::string& src,
                 const std::string& dst,
                 int64_t rank,
                 std::map<std::string, std::string> data) {
  folly::dynamic action = folly::dynamic::object();
  folly::dynamic metadata = folly::dynamic::object();
  folly::dynamic body = folly::dynamic::object();
  auto docId = ESAdapter::genDocID(vid, src, dst, rank);
  metadata["_id"] = docId;
  metadata["_type"] = "_doc";
  metadata["_index"] = indexName;
  action["index"] = std::move(metadata);
  body["vid"] = vid;
  body["src"] = src;
  body["dst"] = dst;
  body["rank"] = rank;
  for (auto& [key, value] : data) {
    body[key] = std::move(value);
  }
  documents_[indexName].emplace_back(std::move(action));
  documents_[indexName].emplace_back(std::move(body));
}

void ESBulk::delete_(const std::string& indexName,
                     const std::string& vid,
                     const std::string& src,
                     const std::string& dst,
                     int64_t rank) {
  folly::dynamic action = folly::dynamic::object();
  folly::dynamic metadata = folly::dynamic::object();
  auto docId = ESAdapter::genDocID(vid, src, dst, rank);
  metadata["_id"] = docId;
  metadata["_type"] = "_doc";
  metadata["_index"] = indexName;
  action["delete"] = std::move(metadata);
  documents_[indexName].emplace_back(std::move(action));
}

bool ESBulk::empty() {
  return documents_.empty();
}

ESAdapter::ESAdapter(std::vector<ESClient>&& clients) : clients_(clients) {}

void ESAdapter::setClients(std::vector<ESClient>&& clients) {
  clients_ = std::move(clients);
}

Status ESAdapter::createIndex(const std::string& name,
                              const std::vector<std::string>& fields,
                              const std::string& analyzer) {
  folly::dynamic obj = folly::parseJson(R"(
    {
      "mappings":{
        "properties":{
          "vid": {
            "type": "keyword"
          },
          "src": {
            "type": "keyword"
          },
          "dst": {
            "type": "keyword"
          },
          "rank": {
            "type": "long"
          }
        }
      }
    }
  )");
  auto& mappings = obj["mappings"];
  for (auto& field : fields) {
    folly::dynamic f = folly::dynamic::object();
    f["type"] = "text";
    if (!analyzer.empty()) {
      f["analyzer"] = analyzer;
    }
    mappings["properties"][field] = std::move(f);
  }

  auto result = randomClient().createIndex(name, obj);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp.count("acknowledged") && resp["acknowledged"].isBool() &&
      resp["acknowledged"].getBool()) {
    return Status::OK();
  }
  if (resp.count("error")) {
    return Status::Error(folly::toJson(resp["error"]));
  }
  return Status::Error(folly::toJson(resp));
}

Status ESAdapter::dropIndex(const std::string& name) {
  auto result = randomClient().dropIndex(name);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp["acknowledged"].isBool() && resp["acknowledged"].getBool()) {
    return Status::OK();
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

Status ESAdapter::clearIndex(const std::string& name, bool refresh) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object("match_all", folly::dynamic::object());
  auto result = randomClient().deleteByQuery(name, body, refresh);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp["failures"].isArray() && resp["failures"].size() == 0) {
    return Status::OK();
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

StatusOr<bool> ESAdapter::isIndexExist(const std::string& name) {
  auto result = randomClient().getIndex(name);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp.count(name)) {
    return true;
  }
  if (resp.count("status") && resp["status"].getInt() == 404) {
    return false;
  }
  if (resp.count("error")) {
    return Status::Error(folly::toJson(resp["error"]));
  }
  return Status::Error(folly::toJson(resp));
}

Status ESAdapter::bulk(const ESBulk& bulk, bool refresh) {
  std::vector<folly::dynamic> jsonArray;
  for (auto& [docId, objs] : bulk.documents_) {
    for (auto& obj : objs) {
      jsonArray.push_back(obj);
    }
  }
  auto result = randomClient().bulk(jsonArray, refresh);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (!resp.count("error")) {
    return Status::OK();
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

StatusOr<ESQueryResult> ESAdapter::queryString(const std::string& index,
                                               const std::string& query,
                                               int64_t from,
                                               int64_t size) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["query_string"] = folly::dynamic::object();
  body["query"]["query_string"]["query"] = query;
  if (size > 0) {
    body["size"] = size;
    body["from"] = from;
  }
  return ESAdapter::query(index, body, 2000);
}

StatusOr<ESQueryResult> ESAdapter::match_all(const std::string& index) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["match_all"] = folly::dynamic::object();
  return ESAdapter::query(index, body, -1);
}

StatusOr<ESQueryResult> ESAdapter::query(const std::string& index,
                                         const folly::dynamic& query,
                                         int64_t timeout) {
  auto result = randomClient().search(index, query, timeout);
  if (!result.ok()) {
    return std::move(result).status();
  }
  auto resp = std::move(result).value();
  if (resp.count("hits")) {
    auto hits = resp["hits"]["hits"];
    ESQueryResult res;
    for (auto& hit : hits) {
      auto source = hit["_source"];
      ESQueryResult::Item item;
      if (hit["_score"].isInt()) {
        item.score = hit["_score"].getInt();
      } else if (hit["_score"].isDouble()) {
        item.score = hit["_score"].getDouble();
      }
      item.src = source["src"].getString();
      item.dst = source["dst"].getString();
      item.rank = source["rank"].getInt();
      item.vid = source["vid"].getString();
      res.items.emplace_back(std::move(item));
    }
    return res;
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

std::string ESAdapter::genDocID(const std::string& vid,
                                const std::string& src,
                                const std::string& dst,
                                int64_t rank) {
  std::string str;
  unsigned char mdStr[32] = {0};
  if (!vid.empty()) {
    str = vid;
  } else {
    str = src + dst + std::to_string(rank);
  }
  SHA256(reinterpret_cast<unsigned char*>(str.data()), str.size(), mdStr);
  char hex[64] = {0};
  for (int i = 0; i < 32; i++) {
    hex[i * 2] = 'A' + mdStr[i] / 16;
    hex[i * 2 + 1] = 'A' + mdStr[i] % 16;
  }
  return std::string(hex, 64);
}

ESClient& ESAdapter::randomClient() {
  static thread_local std::default_random_engine engine;
  static thread_local std::uniform_int_distribution<size_t> d(0, clients_.size() - 1);
  return clients_[d(engine)];
}
}  // namespace nebula::plugin
