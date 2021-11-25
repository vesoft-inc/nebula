/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <unordered_set>

namespace nebula {
namespace graph {

// It's used for cypher properties
class CypherProps final {
 public:
  struct AllProps {
    bool operator==(const AllProps&) const { return true; }
  };
  // 1. All properties of vertex
  // 2. list of properties of vertex
  using Props = std::variant<AllProps, std::set<std::string>>;
  // Alias -> Props
  using AliasProps = std::unordered_map<std::string, Props>;
  // Alias -> labels
  using AliasLabels = std::unordered_map<std::string, std::set<TagID>>;

  CypherProps() = default;
  CypherProps(std::set<std::string> paths,
              AliasProps vertexProps,
              AliasProps edgeProps,
              AliasLabels vertexLabels)
      : paths_(std::move(paths)),
        vertexProps_(std::move(vertexProps)),
        edgeProps_(std::move(edgeProps)),
        vertexLabels_(std::move(vertexLabels)) {}

  bool operator==(const CypherProps& rhs) const {
    if (paths_ != rhs.paths_) {
      return false;
    }
    if (vertexProps_ != rhs.vertexProps_) {
      return false;
    }
    return edgeProps_ == rhs.edgeProps_;
  }

  void addPath(const std::string& path) { paths_.emplace(path); }

  const auto& paths() const { return paths_; }

  void addVertexProp(const std::string& vertex, const std::string& prop) {
    auto find = vertexProps_.find(vertex);
    if (find == vertexProps_.end()) {
      vertexProps_[vertex] = std::set<std::string>();
      std::get<std::set<std::string>>(vertexProps_[vertex]).emplace(prop);
    } else {
      if (!std::holds_alternative<AllProps>(find->second)) {
        std::get<std::set<std::string>>(find->second).emplace(prop);
      }
    }
  }

  void addVertexProp(const std::string& vertex) { vertexProps_[vertex] = AllProps{}; }

  const auto& vertexProps() const { return vertexProps_; }

  void addEdgeProp(const std::string& edge, const std::string& prop) {
    auto find = edgeProps_.find(edge);
    if (find == edgeProps_.end()) {
      edgeProps_[edge] = std::set<std::string>();
      std::get<std::set<std::string>>(edgeProps_[edge]).emplace(prop);
    } else {
      if (!std::holds_alternative<AllProps>(find->second)) {
        std::get<std::set<std::string>>(find->second).emplace(prop);
      }
    }
  }

  void addEdgeProp(const std::string& edge) { edgeProps_[edge] = AllProps{}; }

  const auto& edgeProps() const { return edgeProps_; }

  void addVertexLabel(const std::string& vertex, TagID label) {
    vertexLabels_[vertex].emplace(label);
  }

  const auto& vertexLabels() const { return vertexLabels_; }

 private:
  std::set<std::string> paths_;
  AliasProps vertexProps_;
  AliasProps edgeProps_;
  AliasLabels vertexLabels_;
};

}  // namespace graph
}  // namespace nebula
