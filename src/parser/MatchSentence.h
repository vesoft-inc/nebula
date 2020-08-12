/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_MATCHSENTENCE_H_
#define PARSER_MATCHSENTENCE_H_

#include "common/base/Base.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "parser/Sentence.h"
#include "parser/Clauses.h"

namespace nebula {

class MatchEdgeProp final {
public:
    MatchEdgeProp(std::string *alias, std::string *edge, Expression *props = nullptr) {
        alias_.reset(alias);
        edge_.reset(edge);
        props_.reset(static_cast<MapExpression*>(props));
    }

    auto get() && {
        return std::make_tuple(std::move(alias_), std::move(edge_), std::move(props_));
    }

private:
    std::unique_ptr<std::string>                        alias_;
    std::unique_ptr<std::string>                        edge_;
    std::unique_ptr<MapExpression>                      props_;
};


class MatchEdge final {
public:
    using Direction = nebula::storage::cpp2::EdgeDirection;
    MatchEdge(MatchEdgeProp *prop, Direction direction) {
        if (prop != nullptr) {
            auto tuple = std::move(*prop).get();
            alias_ = std::move(std::get<0>(tuple));
            edge_ = std::move(std::get<1>(tuple));
            props_ = std::move(std::get<2>(tuple));
            delete prop;
        }
        direction_ = direction;
    }

    auto direction() const {
        return direction_;
    }

    const std::string* alias() const {
        return alias_.get();
    }

    const std::string* edge() const {
        return edge_.get();
    }

    const MapExpression* props() const {
        return props_.get();
    }

    std::string toString() const;

private:
    Direction                                       direction_;
    std::unique_ptr<std::string>                    alias_;
    std::unique_ptr<std::string>                    edge_;
    std::unique_ptr<MapExpression>                  props_;
};


class MatchNode final {
public:
    MatchNode(std::string *alias,
              std::string *label = nullptr,
              Expression *props = nullptr) {
        alias_.reset(alias);
        label_.reset(label);
        props_.reset(static_cast<MapExpression*>(props));
    }

    const std::string* alias() const {
        return alias_.get();
    }

    const std::string* label() const {
        return label_.get();
    }

    const MapExpression* props() const {
        return props_.get();
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>                    alias_;
    std::unique_ptr<std::string>                    label_;
    std::unique_ptr<MapExpression>                  props_;
};


class MatchPath final {
public:
    explicit MatchPath(MatchNode *head) {
        head_.reset(head);
    }

    void add(MatchEdge *edge, MatchNode *node) {
        steps_.emplace_back(edge, node);
    }

    const MatchNode* head() const {
        return head_.get();
    }

    using RawStep = std::pair<const MatchEdge*, const MatchNode*>;
    std::vector<RawStep> steps() const {
        std::vector<RawStep> result;
        result.reserve(steps_.size());
        for (auto &step : steps_) {
            result.emplace_back(step.first.get(), step.second.get());
        }
        return result;
    }

    std::string toString() const;

private:
    using Step = std::pair<std::unique_ptr<MatchEdge>, std::unique_ptr<MatchNode>>;
    std::unique_ptr<MatchNode>                      head_;
    std::vector<Step>                               steps_;
};


class MatchReturn final {
public:
    MatchReturn() {
        isAll_ = true;
    }

    explicit MatchReturn(YieldColumns *columns) {
        columns_.reset(columns);
    }

    const YieldColumns* columns() const {
        return columns_.get();
    }

    bool isAll() const {
        return isAll_;
    }

    std::string toString() const;

private:
    std::unique_ptr<YieldColumns>                   columns_;
    bool                                            isAll_{false};
};


class MatchSentence final : public Sentence {
public:
    MatchSentence(MatchPath *path, WhereClause *filter, MatchReturn *ret)
        : Sentence(Kind::kMatch) {
        path_.reset(path);
        filter_.reset(filter);
        return_.reset(ret);
    }

    const MatchPath* path() const {
        return path_.get();
    }

    const WhereClause* filter() const {
        return filter_.get();
    }

    const MatchReturn* ret() const {
        return return_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<MatchPath>                  path_;
    std::unique_ptr<WhereClause>                filter_;
    std::unique_ptr<MatchReturn>                return_;
};

}   // namespace nebula

#endif  // PARSER_MATCHSENTENCE_H_
