/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_COLLECTOR_H_
#define STORAGE_COLLECTOR_H_

#include "base/Base.h"
#include "dataman/RowWriter.h"
#include <boost/variant.hpp>

namespace nebula {
namespace storage {

struct PropContext {
    cpp2::PropDef prop_;
    cpp2::ValueType type_;
    boost::variant<int64_t, double> sum_ = 0L;
    int32_t count_    = 0;
    int32_t retIndex_ = -1;
};

struct TagContext {
    TagID tagId_;
    SchemaProviderIf* schema_ = nullptr;
    std::vector<PropContext> props_;
};

struct EdgeContext {
    EdgeType edgeType_;
    SchemaProviderIf* schema_ = nullptr;
    std::vector<PropContext> props_;
};

class Collector {
public:
    Collector() = default;

    virtual ~Collector() = default;

    virtual void collectInt32(ResultType ret, int32_t v, PropContext& prop) = 0;

    virtual void collectInt64(ResultType ret, int64_t v, PropContext& prop) = 0;

    virtual void collectFloat(ResultType ret, float v, PropContext& prop) = 0;

    virtual void collectDouble(ResultType ret, double v, PropContext& prop) = 0;

    virtual void collectString(ResultType ret, folly::StringPiece& v, PropContext& prop) = 0;
};

class PropsCollector : public Collector {
public:
    PropsCollector(RowWriter* writer)
        : writer_(writer) {}

    void collectInt32(ResultType ret, int32_t v, PropContext& prop) override {
        collect<int32_t>(ret, v, prop);
    }

    void collectInt64(ResultType ret, int64_t v, PropContext& prop) override {
        collect<int64_t>(ret, v, prop);
    }

    void collectFloat(ResultType ret, float v, PropContext& prop) override {
        collect<float>(ret, v, prop);
    }

    void collectDouble(ResultType ret, double v, PropContext& prop) override {
        collect<double>(ret, v, prop);
    }

    void collectString(ResultType ret, folly::StringPiece& v, PropContext& prop) override {
        collect<folly::StringPiece>(ret, v, prop);
    }

    template<typename V>
    void collect(ResultType ret, V& v, PropContext& prop) {
        UNUSED(prop);
        if (ResultType::SUCCEEDED == ret) {
            (*writer_) << v;
        }
    }

private:
    RowWriter* writer_ = nullptr;
};

class StatsCollector : public Collector {
public:
    StatsCollector() = default;

    void collectInt32(ResultType ret, int32_t v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectInt64(ResultType ret, int64_t v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectFloat(ResultType ret, float v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<double>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectDouble(ResultType ret, double v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<double>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectString(ResultType ret, folly::StringPiece& v, PropContext& prop) override {
        UNUSED(v);
        if (ret == ResultType::SUCCEEDED) {
            prop.count_++;
        }
    }

};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COLLECTOR_H_

