/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/PathBuildExpression.h"

#include "common/datatypes/Path.h"
#include "common/expression/ExprVisitor.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {
const Value& PathBuildExpression::eval(ExpressionContext& ctx) {
  if (items_.empty()) {
    return Value::kNullValue;
  }
  Path path;
  auto& val = items_.front()->eval(ctx);
  if (!getVertex(val, path.src)) {
    return Value::kNullBadType;
  }
  if (val.isPath()) {
    const auto& steps = val.getPath().steps;
    path.steps.insert(path.steps.end(), steps.begin(), steps.end());
  }

  for (size_t i = 1; i < items_.size(); ++i) {
    auto& value = items_[i]->eval(ctx);
    if (!buildPath(value, path)) {
      return Value::kNullBadData;
    }
  }

  result_ = path;
  return result_;
}

bool PathBuildExpression::buildPath(const Value& value, Path& path) const {
  switch (value.type()) {
    case Value::Type::EDGE: {
      Step step;
      if (!path.steps.empty()) {
        getEdge(value, path.steps.back().dst.vid, step);
      } else {
        getEdge(value, path.src.vid, step);
      }
      path.steps.emplace_back(std::move(step));
      break;
    }
    case Value::Type::VERTEX: {
      if (path.steps.empty()) {
        if (path.src.vid == value.getVertex().vid) {
          return true;
        }
        return false;
      }
      auto& lastStep = path.steps.back();
      getVertex(value, lastStep.dst);
      break;
    }
    case Value::Type::PATH: {
      const auto& p = value.getPath();
      if (!path.steps.empty()) {
        auto& lastStep = path.steps.back();
        lastStep.dst = p.src;
      }
      path.steps.insert(path.steps.end(), p.steps.begin(), p.steps.end());
      break;
    }
    case Value::Type::LIST: {
      for (const auto& val : value.getList().values) {
        if (!buildPath(val, path)) {
          return false;
        }
      }
      break;
    }
    default: {
      return false;
    }
  }
  return true;
}

bool PathBuildExpression::getVertex(const Value& value, Vertex& vertex) const {
  if (value.isStr() || value.isInt()) {
    vertex = Vertex(value, {});
    return true;
  }
  if (value.isVertex()) {
    vertex = Vertex(value.getVertex());
    return true;
  }
  if (value.isPath()) {
    vertex = value.getPath().src;
    return true;
  }
  return false;
}

bool PathBuildExpression::getEdge(const Value& value, const Value& lastStepVid, Step& step) const {
  if (!value.isEdge()) {
    return false;
  }

  const auto& edge = value.getEdge();
  if (lastStepVid == edge.src) {
    step.type = edge.type;
    step.name = edge.name;
    step.ranking = edge.ranking;
    step.props = edge.props;
    step.dst.vid = edge.dst;
  } else {
    step.type = -edge.type;
    step.name = edge.name;
    step.ranking = edge.ranking;
    step.props = edge.props;
    step.dst.vid = edge.src;
  }
  return true;
}

bool PathBuildExpression::operator==(const Expression& rhs) const {
  if (kind() != rhs.kind()) {
    return false;
  }

  auto& pathBuild = static_cast<const PathBuildExpression&>(rhs);
  if (length() != pathBuild.length()) {
    return false;
  }

  for (size_t i = 0; i < size(); ++i) {
    if (*items_[i] != *pathBuild.items_[i]) {
      return false;
    }
  }

  return true;
}

std::string PathBuildExpression::toString() const {
  std::string buf;
  buf.reserve(256);

  buf += "PathBuild[";
  for (auto& item : items_) {
    buf += item->toString();
    buf += ",";
  }
  buf.back() = ']';
  return buf;
}

void PathBuildExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

Expression* PathBuildExpression::clone() const {
  auto pathBuild = PathBuildExpression::make(pool_);
  for (auto& item : items_) {
    pathBuild->add(item->clone());
  }
  return pathBuild;
}

void PathBuildExpression::writeTo(Encoder& encoder) const {
  encoder << kind();
  encoder << size();
  for (auto& item : items_) {
    encoder << *item;
  }
}

void PathBuildExpression::resetFrom(Decoder& decoder) {
  auto size = decoder.readSize();
  items_.reserve(size);
  for (auto i = 0u; i < size; ++i) {
    auto item = decoder.readExpression(pool_);
    items_.emplace_back(item);
  }
}
}  // namespace nebula
