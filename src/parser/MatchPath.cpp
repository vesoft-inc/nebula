/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/MatchPath.h"

namespace nebula {

std::string MatchStepRange::toString() const {
  return folly::stringPrintf("%lu..%lu", min(), max());
}

std::string MatchEdge::toString() const {
  std::string buf;
  buf.reserve(256);

  std::string end;
  if (direction_ == Direction::OUT_EDGE) {
    buf += '-';
    end = "->";
  } else if (direction_ == Direction::IN_EDGE) {
    buf += "<-";
    end = "-";
  } else {
    buf += '-';
    end = "-";
  }

  if (!alias_.empty() || !types_.empty() || range_ != nullptr || props_ != nullptr) {
    buf += '[';
    if (!alias_.empty()) {
      buf += alias_;
    }
    if (!types_.empty()) {
      buf += ':';
      buf += *types_[0];
      for (auto i = 1u; i < types_.size(); i++) {
        buf += "|";
        buf += *types_[i];
      }
    }
    if (range_ != nullptr) {
      buf += "*";
      if (range_->min() == range_->max()) {
        buf += folly::to<std::string>(range_->min());
      } else if (range_->max() == std::numeric_limits<size_t>::max()) {
        if (range_->min() != 1) {
          buf += folly::to<std::string>(range_->min());
          buf += "..";
        }
      } else {
        if (range_->min() != 1) {
          buf += folly::to<std::string>(range_->min());
        }
        buf += "..";
        buf += folly::to<std::string>(range_->max());
      }
    }
    if (props_ != nullptr) {
      buf += props_->toString();
    }
    buf += ']';
  }

  buf += end;

  return buf;
}

std::string MatchNode::toString() const {
  std::string buf;
  buf.reserve(64);

  buf += '(';
  if (!alias_.empty()) {
    buf += alias_;
  }
  if (labels_ != nullptr) {
    buf += labels_->toString();
  }
  if (props_ != nullptr) {
    buf += props_->toString();
  }
  buf += ')';

  return buf;
}

std::string MatchPath::toString() const {
  std::string buf;
  buf.reserve(256);

  if (alias_ != nullptr) {
    buf += *alias_;
    buf += " = ";
  }

  buf += node(0)->toString();
  for (auto i = 0u; i < edges_.size(); i++) {
    buf += edge(i)->toString();
    buf += node(i + 1)->toString();
  }

  return buf;
}

}  // namespace nebula
