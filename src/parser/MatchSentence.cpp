/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/MatchSentence.h"

namespace nebula {

std::string MatchStepRange::toString() const {
  return folly::stringPrintf("%lu..%lu", min(), max());
}

std::string MatchClause::toString() const {
  std::string buf;
  buf.reserve(256);

  if (isOptional()) {
    buf += "OPTIONAL ";
  }

  buf += "MATCH ";
  buf += pathList_->toString();
  if (where_ != nullptr) {
    buf += " ";
    buf += where_->toString();
  }
  return buf;
}

std::string UnwindClause::toString() const {
  std::string buf;
  buf.reserve(256);

  buf += "UNWIND ";
  buf += expr_->toString();
  buf += " AS ";
  buf += alias_;

  return buf;
}

std::string WithClause::toString() const {
  std::string buf;
  buf.reserve(256);

  buf += "WITH ";

  if (isDistinct()) {
    buf += "DISTINCT ";
  }

  buf += returnItems_->toString();

  if (orderFactors_ != nullptr) {
    buf += " ";
    buf += "ORDER BY ";
    buf += orderFactors_->toString();
  }

  if (skip_ != nullptr) {
    buf += " ";
    buf += "SKIP ";
    buf += skip_->toString();
  }

  if (limit_ != nullptr) {
    buf += " ";
    buf += "LIMIT ";
    buf += limit_->toString();
  }

  if (where_ != nullptr) {
    buf += " ";
    buf += where_->toString();
  }

  return buf;
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

std::string MatchReturnItems::toString() const {
  std::string buf;
  if (allNamedAliases_) {
    buf += '*';
  }
  if (columns_) {
    if (allNamedAliases_) {
      buf += ',';
    }
    buf += columns_->toString();
  }

  return buf;
}

std::string MatchReturn::toString() const {
  std::string buf;
  buf.reserve(64);

  buf += "RETURN ";

  if (isDistinct()) {
    buf += "DISTINCT ";
  }

  buf += returnItems_->toString();

  if (orderFactors_ != nullptr) {
    buf += " ";
    buf += "ORDER BY ";
    buf += orderFactors_->toString();
  }

  if (skip_ != nullptr) {
    buf += " ";
    buf += "SKIP ";
    buf += skip_->toString();
  }

  if (limit_ != nullptr) {
    buf += " ";
    buf += "LIMIT ";
    buf += limit_->toString();
  }

  return buf;
}

std::string MatchSentence::toString() const {
  std::string buf;
  buf.reserve(256);

  for (auto& clause : clauses_) {
    buf += clause->toString();
    buf += " ";
  }

  buf += return_->toString();

  return buf;
}

MatchPathList::MatchPathList(MatchPath* path) {
  pathList_.emplace_back(path);
}

void MatchPathList::add(MatchPath* path) {
  pathList_.emplace_back(path);
}

std::string MatchPathList::toString() const {
  std::string buf;
  buf.reserve(256);
  std::vector<std::string> pathList;
  std::transform(pathList_.begin(), pathList_.end(), std::back_inserter(pathList), [](auto& path) {
    return path->toString();
  });
  folly::join(",", pathList.begin(), pathList.end(), buf);
  return buf;
}

}  // namespace nebula
