/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
        if (value.isEdge()) {
            if (!path.steps.empty()) {
                const auto& lastStep = path.steps.back();
                const auto& edge = value.getEdge();
                if (lastStep.dst.vid != edge.src) {
                    return Value::kNullBadData;
                }
            }
            Step step;
            getEdge(value, step);
            path.steps.emplace_back(std::move(step));
        } else if (value.isVertex()) {
            if (path.steps.empty()) {
                return Value::kNullBadData;
            }
            auto& lastStep = path.steps.back();
            const auto& vert = value.getVertex();
            if (lastStep.dst.vid != vert.vid) {
                return Value::kNullBadData;
            }
            getVertex(value, lastStep.dst);
        } else if (value.isPath()) {
            const auto& p = value.getPath();
            if (!path.steps.empty()) {
                auto& lastStep = path.steps.back();
                if (lastStep.dst.vid != p.src.vid) {
                    return Value::kNullBadData;
                }
                lastStep.dst = p.src;
            }
            path.steps.insert(path.steps.end(), p.steps.begin(), p.steps.end());
        } else {
            if ((i & 1) == 1 || path.steps.empty() || !getVertex(value, path.steps.back().dst)) {
                return Value::kNullBadData;
            }
        }
    }

    result_ = path;
    return result_;
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

bool PathBuildExpression::getEdge(const Value& value, Step& step) const {
    if (value.isEdge()) {
        const auto& edge = value.getEdge();
        step.type = edge.type;
        step.name = edge.name;
        step.ranking = edge.ranking;
        step.props = edge.props;
        step.dst.vid = edge.dst;
        return true;
    }

    return false;
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

void PathBuildExpression::writeTo(Encoder &encoder) const {
    encoder << kind();
    encoder << size();
    for (auto& item : items_) {
        encoder << *item;
    }
}

void PathBuildExpression::resetFrom(Decoder &decoder) {
    auto size = decoder.readSize();
    items_.reserve(size);
    for (auto i = 0u; i < size; ++i) {
        auto item = decoder.readExpression(pool_);
        items_.emplace_back(item);
    }
}
}  // namespace nebula
