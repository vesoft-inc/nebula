/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef PARSER_SENTENCE_H_
#define PARSER_SENTENCE_H_

#include "common/base/Base.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/TextSearchExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {

class Sentence {
 public:
  virtual ~Sentence() {}
  virtual std::string toString() const = 0;

  enum class Kind : uint32_t {
    kUnknown,
    kExplain,
    kSequential,
    kGo,
    kSet,
    kPipe,
    kUse,
    kMatch,
    kAssignment,
    kCreateTag,
    kAlterTag,
    kCreateEdge,
    kAlterEdge,
    kDescribeTag,
    kDescribeEdge,
    kCreateTagIndex,
    kCreateEdgeIndex,
    kDropTagIndex,
    kDropEdgeIndex,
    kDescribeTagIndex,
    kDescribeEdgeIndex,
    kDropTag,
    kDropEdge,
    kInsertVertices,
    kUpdateVertex,
    kInsertEdges,
    kUpdateEdge,
    kAddHosts,
    kDropHosts,
    kShowHosts,
    kShowSpaces,
    kShowParts,
    kShowTags,
    kShowEdges,
    kShowTagIndexes,
    kShowEdgeIndexes,
    kShowTagIndexStatus,
    kShowEdgeIndexStatus,
    kShowUsers,
    kShowRoles,
    kShowCreateSpace,
    kShowCreateTag,
    kShowCreateEdge,
    kShowCreateTagIndex,
    kShowCreateEdgeIndex,
    kShowSnapshots,
    kShowCharset,
    kShowCollation,
    kShowGroups,
    kShowZones,
    kShowStats,
    kShowServiceClients,
    kShowFTIndexes,
    kDescribeUser,
    kDeleteVertices,
    kDeleteTags,
    kDeleteEdges,
    kLookup,
    kCreateSpace,
    kCreateSpaceAs,
    kDropSpace,
    kDescribeSpace,
    kYield,
    kCreateUser,
    kDropUser,
    kAlterUser,
    kGrant,
    kRevoke,
    kChangePassword,
    kOrderBy,
    kShowConfigs,
    kSetConfig,
    kGetConfig,
    kFetchVertices,
    kFetchEdges,
    kFindPath,
    kLimit,
    kGroupBy,
    kReturn,
    kCreateSnapshot,
    kDropSnapshot,
    kAdminJob,
    kAdminShowJobs,
    kGetSubgraph,
    kMergeZone,
    kRenameZone,
    kDropZone,
    kDivideZone,
    kDescribeZone,
    kListZones,
    kAddHostsIntoZone,
    kAddListener,
    kRemoveListener,
    kShowListener,
    kSignInService,
    kSignOutService,
    kCreateFTIndex,
    kDropFTIndex,
    kShowSessions,
    kKillSession,
    kShowQueries,
    kKillQuery,
    kShowMetaLeader,
    kAlterSpace,
    kClearSpace,
    kUnwind,
  };

  Kind kind() const {
    return kind_;
  }

 protected:
  Sentence() = default;
  explicit Sentence(Kind kind) : kind_(kind) {}

  Kind kind_{Kind::kUnknown};
};

class CreateSentence : public Sentence {
 public:
  explicit CreateSentence(bool ifNotExist) : ifNotExist_{ifNotExist} {}
  virtual ~CreateSentence() {}

  bool isIfNotExist() const {
    return ifNotExist_;
  }

 private:
  bool ifNotExist_{false};
};

class DropSentence : public Sentence {
 public:
  explicit DropSentence(bool ifExists) : ifExists_{ifExists} {}
  virtual ~DropSentence() = default;

  bool isIfExists() {
    return ifExists_;
  }

 private:
  bool ifExists_{false};
};

class HostList final {
 public:
  void addHost(HostAddr *addr) {
    hosts_.emplace_back(addr);
  }

  std::string toString() const;

  std::vector<HostAddr> hosts() const {
    std::vector<HostAddr> result;
    result.reserve(hosts_.size());
    for (auto &host : hosts_) {
      result.emplace_back(*host);
    }
    return result;
  }

 private:
  std::vector<std::unique_ptr<HostAddr>> hosts_;
};

inline std::ostream &operator<<(std::ostream &os, Sentence::Kind kind) {
  return os << static_cast<uint32_t>(kind);
}

class ZoneNameList final {
 public:
  ZoneNameList() = default;

  void addZone(std::string *zone) {
    zones_.emplace_back(zone);
  }

  std::vector<std::string> zoneNames() const {
    std::vector<std::string> result;
    result.resize(zones_.size());
    auto get = [](auto &ptr) { return *ptr.get(); };
    std::transform(zones_.begin(), zones_.end(), result.begin(), get);
    return result;
  }

  std::string toString() const {
    std::string buf;
    for (const auto &zone : zones_) {
      buf += "`";
      buf += *zone;
      buf += "`";
      buf += ",";
    }
    if (!zones_.empty()) {
      buf.pop_back();
    }
    return buf;
  }

 private:
  std::vector<std::unique_ptr<std::string>> zones_;
};

class ZoneItem final {
 public:
  ZoneItem(std::string *zone, HostList *hosts) {
    zone_.reset(zone);
    hosts_.reset(hosts);
  }

  explicit ZoneItem(ZoneItem *other)
      : zone_(std::move(other->zone_)), hosts_(std::move(other->hosts_)) {}

  std::string toString() const {
    std::string buf;
    buf += "`";
    buf += *zone_;
    buf += "`";
    buf += " (";
    buf += hosts_->toString();
    buf += ")";
    return buf;
  }

  std::string *zone() {
    return zone_.get();
  }

  HostList *hosts() {
    return hosts_.get();
  }

 private:
  std::unique_ptr<std::string> zone_;
  std::unique_ptr<HostList> hosts_;
};

class ZoneItemList final {
 public:
  ZoneItemList() = default;

  void addZoneItem(ZoneItem *zoneItem) {
    zoneItems_.emplace_back(zoneItem);
  }

  std::unordered_map<std::string, std::vector<HostAddr>> zoneItems() const {
    std::unordered_map<std::string, std::vector<HostAddr>> result;
    for (const auto &item : zoneItems_) {
      result.emplace(*item->zone(), item->hosts()->hosts());
    }
    return result;
  }

  std::string toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : zoneItems_) {
      buf += item->toString();
      buf += " ";
    }

    if (!buf.empty()) {
      buf.pop_back();
    }
    return buf;
  }

 private:
  std::vector<std::unique_ptr<ZoneItem>> zoneItems_;
};

}  // namespace nebula

#endif  // PARSER_SENTENCE_H_
