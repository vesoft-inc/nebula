/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef PARSER_MATCHPATH_H_
#define PARSER_MATCHPATH_H_

#include <memory>
#include <string>
#include <vector>

#include "common/expression/ContainerExpression.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {

class MatchEdgeTypeList final {
 public:
  MatchEdgeTypeList() = default;

  void add(std::string* item) {
    items_.emplace_back(item);
  }

  auto items() && {
    return std::move(items_);
  }

 private:
  std::vector<std::unique_ptr<std::string>> items_;
};

class MatchStepRange final {
 public:
  explicit MatchStepRange(size_t min = 0, size_t max = std::numeric_limits<size_t>::max()) {
    min_ = min;
    max_ = max;
  }

  auto min() const {
    return min_;
  }

  auto max() const {
    return max_;
  }

  std::string toString() const;

 private:
  size_t min_{1};
  size_t max_{1};
};

class MatchEdgeProp final {
 public:
  MatchEdgeProp(const std::string& alias,
                MatchEdgeTypeList* types,
                MatchStepRange* range,
                Expression* props = nullptr) {
    alias_ = alias;
    range_.reset(range);
    props_ = static_cast<MapExpression*>(props);
    if (types != nullptr) {
      types_ = std::move(*types).items();
      delete types;
    }
  }

  auto get() && {
    return std::make_tuple(
        std::move(alias_), std::move(types_), std::move(range_), std::move(props_));
  }

 private:
  std::string alias_;
  std::vector<std::unique_ptr<std::string>> types_;
  MapExpression* props_{nullptr};
  std::unique_ptr<MatchStepRange> range_;
};

class MatchEdge final {
 public:
  using Direction = nebula::storage::cpp2::EdgeDirection;
  MatchEdge(MatchEdgeProp* prop, Direction direction) {
    if (prop != nullptr) {
      auto tuple = std::move(*prop).get();
      alias_ = std::move(std::get<0>(tuple));
      types_ = std::move(std::get<1>(tuple));
      range_ = std::move(std::get<2>(tuple));
      props_ = std::move(std::get<3>(tuple));
      delete prop;
    }
    direction_ = direction;
  }
  MatchEdge() = default;

  auto direction() const {
    return direction_;
  }

  const std::string& alias() const {
    return alias_;
  }

  auto& types() const {
    return types_;
  }

  const MapExpression* props() const {
    return props_;
  }

  MatchStepRange* range() const {
    return range_.get();
  }

  std::string toString() const;

  MatchEdge clone() const {
    auto me = MatchEdge();
    me.direction_ = direction_;
    me.alias_ = alias_;
    for (const auto& type : types_) {
      me.types_.emplace_back(std::make_unique<std::string>(*DCHECK_NOTNULL(type)));
    }
    if (range_ != nullptr) {
      me.range_ = std::make_unique<MatchStepRange>(*range_);
    }
    if (props_ != nullptr) {
      me.props_ = static_cast<MapExpression*>(props_->clone());
    }
    return me;
  }

 private:
  Direction direction_;
  std::string alias_;
  std::vector<std::unique_ptr<std::string>> types_;
  std::unique_ptr<MatchStepRange> range_;
  MapExpression* props_{nullptr};
};

class MatchNodeLabel final {
 public:
  explicit MatchNodeLabel(std::string* label, Expression* props = nullptr)
      : label_(label), props_(static_cast<MapExpression*>(props)) {
    DCHECK(props == nullptr || props->kind() == Expression::Kind::kMap);
  }
  MatchNodeLabel() = default;

  const std::string* label() const {
    return label_.get();
  }

  const MapExpression* props() const {
    return props_;
  }

  MapExpression* props() {
    return props_;
  }

  std::string toString() const {
    std::stringstream ss;
    ss << ":" << *label_;
    if (props_ != nullptr) {
      ss << props_->toString();
    }
    return ss.str();
  }

  MatchNodeLabel clone() const {
    auto mnl = MatchNodeLabel();
    if (label_ != nullptr) {
      mnl.label_ = std::make_unique<std::string>(*label_);
    }
    if (props_ != nullptr) {
      mnl.props_ = static_cast<MapExpression*>(props_->clone());
    }
    return mnl;
  }

 private:
  std::unique_ptr<std::string> label_;
  MapExpression* props_{nullptr};
};

class MatchNodeLabelList final {
 public:
  void add(MatchNodeLabel* label) {
    labels_.emplace_back(label);
  }

  const auto& labels() const {
    return labels_;
  }

  std::string toString() const {
    std::stringstream ss;
    for (const auto& label : labels_) {
      ss << label->toString();
    }
    return ss.str();
  }

  MatchNodeLabelList clone() const {
    MatchNodeLabelList ret;
    for (const auto& label : labels_) {
      ret.labels_.emplace_back(std::make_unique<MatchNodeLabel>(label->clone()));
    }
    return ret;
  }

 private:
  std::vector<std::unique_ptr<MatchNodeLabel>> labels_;
};

class MatchNode final {
 public:
  MatchNode(const std::string& alias = "",
            MatchNodeLabelList* labels = nullptr,
            Expression* props = nullptr) {
    alias_ = alias;
    labels_.reset(labels);
    props_ = static_cast<MapExpression*>(props);
  }

  void setAlias(const std::string& alias) {
    alias_ = alias;
  }

  const std::string& alias() const {
    return alias_;
  }

  const auto* labels() const {
    return labels_.get();
  }

  const MapExpression* props() const {
    return props_;
  }

  MapExpression* props() {
    return props_;
  }

  std::string toString() const;

  MatchNode clone() const {
    auto me = MatchNode();
    me.alias_ = alias_;
    if (labels_ != nullptr) {
      me.labels_ = std::make_unique<MatchNodeLabelList>(labels_->clone());
    }
    if (props_ != nullptr) {
      me.props_ = static_cast<MapExpression*>(props_->clone());
    }
    return me;
  }

  enum class VariableDefinedSource {
    kUnknown,
    kExpression,   // from upper expression
    kMatchClause,  // from previous match clause
  };

  VariableDefinedSource variableDefinedSource() const {
    return variableDefinedSource_;
  }

  void setVariableDefinedSource(VariableDefinedSource source) {
    variableDefinedSource_ = source;
  }

 private:
  std::string alias_;
  std::unique_ptr<MatchNodeLabelList> labels_;
  MapExpression* props_{nullptr};
  // Only used for pattern expression
  VariableDefinedSource variableDefinedSource_{VariableDefinedSource::kUnknown};
};

class MatchPath final {
 public:
  MatchPath() = default;
  explicit MatchPath(MatchNode* node) {
    nodes_.emplace_back(node);
  }

  void add(MatchEdge* edge, MatchNode* node) {
    edges_.emplace_back(edge);
    nodes_.emplace_back(node);
  }

  void setAlias(std::string* alias) {
    alias_.reset(alias);
  }

  const std::string* alias() const {
    return alias_.get();
  }

  auto& nodes() {
    return nodes_;
  }

  const auto& nodes() const {
    return nodes_;
  }

  const auto& edges() const {
    return edges_;
  }

  size_t steps() const {
    return edges_.size();
  }

  const MatchNode* node(size_t i) const {
    return nodes_[i].get();
  }

  const MatchEdge* edge(size_t i) const {
    return edges_[i].get();
  }

  enum PathType : int8_t { kDefault, kAllShortest, kSingleShortest };

  PathType pathType() const {
    return pathType_;
  }

  void setPathType(PathType type) {
    pathType_ = type;
  }

  bool isPredicate() const {
    return isPred_;
  }

  void setPredicate() {
    isPred_ = true;
  }

  bool isAntiPredicate() const {
    return isPred_ && isAntiPred_;
  }

  void setAntiPredicate() {
    isPred_ = true;
    isAntiPred_ = true;
  }

  std::string toString() const;

  MatchPath clone() const {
    auto path = MatchPath(new MatchNode(nodes_[0]->clone()));
    if (alias_ != nullptr) {
      path.setAlias(new std::string(*alias_));
    }
    for (std::size_t i = 0; i < edges_.size(); ++i) {
      path.add(new MatchEdge(edges_[i]->clone()), new MatchNode(nodes_[i + 1]->clone()));
    }
    return path;
  }

 private:
  std::unique_ptr<std::string> alias_;
  std::vector<std::unique_ptr<MatchNode>> nodes_;
  std::vector<std::unique_ptr<MatchEdge>> edges_;
  PathType pathType_{PathType::kDefault};

  bool isPred_{false};
  bool isAntiPred_{false};
};

}  // namespace nebula

#endif  // PARSER_MATCHPATH_H_
