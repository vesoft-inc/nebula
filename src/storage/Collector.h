/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COLLECTOR_H_
#define STORAGE_COLLECTOR_H_

#include "base/Base.h"
#include "dataman/RowWriter.h"
#include <boost/variant.hpp>
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

class Collector {
public:
    Collector() = default;

    virtual ~Collector() = default;

    virtual void collectDstId(VertexID) {}

    virtual void collectVid(int64_t v, const PropContext& prop) = 0;

    virtual void collectBool(bool v, const PropContext& prop) = 0;

    virtual void collectInt64(int64_t v, const PropContext& prop) = 0;

    virtual void collectDouble(double v, const PropContext& prop) = 0;

    virtual void collectString(const std::string& v, const PropContext& prop) = 0;
};


class PropsCollector : public Collector {
public:
    explicit PropsCollector(RowWriter* writer)
        : writer_(writer) {}

    void collectDstId(VertexID dstId) override {
        dstId_ = dstId;
    }

    void collectVid(int64_t v, const PropContext& prop) override {
        (*writer_) << v;
        VLOG(3) << "collect vid: " << prop.prop_.name << ", value = " << v;
    }

    void collectBool(bool v, const PropContext& prop) override {
        (*writer_) << v;
        VLOG(3) << "collect bool: " << prop.prop_.name << ", value = " << v;
    }

    void collectInt64(int64_t v, const PropContext& prop) override {
        (*writer_) << v;
        VLOG(3) << "collect int: " << prop.prop_.name << ", value = " << v;
    }

    void collectDouble(double v, const PropContext& prop) override {
        (*writer_) << v;
        VLOG(3) << "collect double: " << prop.prop_.name << ", value = " << v;
    }

    void collectString(const std::string& v, const PropContext& prop) override {
        (*writer_) << v;
        VLOG(3) << "collect string: " << prop.prop_.name << ", value = " << v;
    }

    template<typename V>
    void collect(V& v, const PropContext&) {
        (*writer_) << v;
    }

    VertexID getDstId() const {
        return dstId_;
    }

private:
    VertexID dstId_;
    RowWriter* writer_;
};


class StatsCollector : public Collector {
public:
    StatsCollector() = default;

    void collectVid(int64_t v, const PropContext& prop) override {
        UNUSED(v);
        UNUSED(prop);
    }

    void collectBool(bool, const PropContext& prop) override {
        std::lock_guard<std::mutex> lg(lock_);
        prop.count_++;
    }

    void collectInt64(int64_t v, const PropContext& prop) override {
        std::lock_guard<std::mutex> lg(lock_);
        prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
        prop.count_++;
    }

    void collectDouble(double v, const PropContext& prop) override {
        std::lock_guard<std::mutex> lg(lock_);
        prop.sum_ = boost::get<double>(prop.sum_) + v;
        prop.count_++;
    }

    void collectString(const std::string&, const PropContext& prop) override {
        std::lock_guard<std::mutex> lg(lock_);
        prop.count_++;
    }

private:
    std::mutex lock_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COLLECTOR_H_
