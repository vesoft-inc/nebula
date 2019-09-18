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

    void collectVid(int64_t v, const PropContext& prop) override {
        UNUSED(prop);
        (*writer_) << RowWriter::ColType(nebula::cpp2::SupportedType::VID) << v;
    }

    void collectBool(bool v, const PropContext& prop) override {
        UNUSED(prop);
        (*writer_) << RowWriter::ColType(nebula::cpp2::SupportedType::BOOL) << v;
    }

    void collectInt64(int64_t v, const PropContext& prop) override {
        UNUSED(prop);
        (*writer_) << RowWriter::ColType(nebula::cpp2::SupportedType::INT) << v;
    }

    void collectDouble(double v, const PropContext& prop) override {
        UNUSED(prop);
        (*writer_) << RowWriter::ColType(nebula::cpp2::SupportedType::DOUBLE) << v;
    }

    void collectString(const std::string& v, const PropContext& prop) override {
        UNUSED(prop);
        (*writer_) << RowWriter::ColType(nebula::cpp2::SupportedType::STRING) << v;
    }

    template<typename V>
    void collect(V& v, const PropContext&) {
        (*writer_) << v;
    }

private:
    RowWriter* writer_ = nullptr;
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
