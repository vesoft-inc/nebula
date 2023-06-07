/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_ESADAPTER_H_
#define COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_ESADAPTER_H_

#include <ostream>

#include "common/base/StatusOr.h"
#include "common/plugin/fulltext/elasticsearch/ESClient.h"
#include "folly/container/F14Map.h"
#include "folly/dynamic.h"
namespace nebula::plugin {

struct ESQueryResult {
  struct Item {
    Item() = default;
    Item(const std::string& vid, double score);
    Item(const std::string& src, const std::string& dst, int64_t rank, double score);

    std::string vid;       // for vertex
    std::string src, dst;  // for edge
    int64_t rank = 0;      // for edge
    double score;

    bool operator==(const Item& item) const {
      return vid == item.vid && src == item.src && dst == item.dst && rank == item.rank &&
             score == item.score;
    }
    std::ostream& operator<<(std::ostream& out) {
      out << fmt::format("vid={}, src={}, dst={}, rank={}, score={}", vid, src, dst, rank, score);
      return out;
    }
  };
  std::vector<Item> items;
};

class ESBulk {
 public:
  void put(const std::string& indexName,
           const std::string& vid,
           const std::string& src,
           const std::string& dst,
           int64_t rank,
           const std::string& text);
  void delete_(const std::string& indexName,
               const std::string& vid,
               const std::string& src,
               const std::string& dst,
               int64_t rank);

  bool empty();

 private:
  folly::F14FastMap<std::string, std::vector<folly::dynamic>> documents_;
  friend class ESAdapter;
};

class ESAdapter {
 public:
  explicit ESAdapter(std::vector<ESClient>&& clients);
  ESAdapter() = default;
  ESAdapter(const ESAdapter& adapter) : clients_(adapter.clients_) {}
  ESAdapter& operator=(const ESAdapter& adapter) {
    clients_ = adapter.clients_;
    return *this;
  }
  virtual ~ESAdapter() = default;
  virtual void setClients(std::vector<ESClient>&& clients);
  virtual Status createIndex(const std::string& name,
                             const std::vector<std::string>& fields,
                             const std::string& analyzer);
  virtual Status dropIndex(const std::string& name);
  virtual Status clearIndex(const std::string& name, bool refresh = false);
  virtual StatusOr<bool> isIndexExist(const std::string& name);

  virtual Status bulk(const ESBulk& bulk, bool refresh = false);

  virtual StatusOr<ESQueryResult> queryString(const std::string& index,
                                              const std::string& query,
                                              const std::vector<std::string>& fields,
                                              int64_t from,
                                              int64_t size);

  virtual StatusOr<ESQueryResult> match_all(const std::string& index);
  StatusOr<ESQueryResult> query(const std::string& index,
                                const folly::dynamic& query,
                                int64_t timeout);

 protected:
  static std::string genDocID(const std::string& vid,
                              const std::string& src,
                              const std::string& dst,
                              int64_t rank);

  ESClient& randomClient();
  std::vector<ESClient> clients_;

  friend class ESBulk;
};

}  // namespace nebula::plugin

#endif
