/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FILTER_TIMEFUNCTION_H
#define COMMON_FILTER_TIMEFUNCTION_H

#include "TimeType.h"
#include "base/StatusOr.h"
#include "base/Status.h"
#include "Expressions.h"

/**
 * Time functions can calculate from 1400 years, and the ones before 1400 years are not supported
 */

namespace nebula {
class TimeCommon final {
public:
    TimeCommon() = default;
    ~TimeCommon() = default;

public:
    static StatusOr<Timezone> getTimezone(const std::string &timezoneStr);

    // get current datetime
    static NebulaTime getUTCTime(bool decimals = false);

    // return true if value is Ok
    static bool checkDatetime(const NebulaTime &nTime);

    static StatusOr<NebulaTime> getDate(int64_t date);

    static StatusOr<NebulaTime> getTime(int64_t time);

    static StatusOr<NebulaTime> getDateTime(int64_t time);

    static StatusOr<NebulaTime> getTimestamp(int64_t time);

    static StatusOr<NebulaTime> isDate(const std::string &value);

    static StatusOr<NebulaTime> isTime(const std::string &value);

    static StatusOr<NebulaTime> isDateTime(const std::string &value);

    static bool isLeapyear(int32_t year);

    // covert 20191201101000 to NebulaTime
    static StatusOr<NebulaTime> intToNebulaTime(int64_t value, DateTimeType type = DataTimeType);

    // covert "20191201101000" to NebulaTime
    static StatusOr<NebulaTime> strToNebulaTime1(const std::string &value,
                                                 DateTimeType type = DataTimeType);

    // covert "2019-12-01 10:10:00" to NebulaTime
    static StatusOr<NebulaTime> strToNebulaTime2(const std::string &value,
                                                 DateTimeType type = DataTimeType);

    // covert OptVariantType to NebulaTime
    static StatusOr<NebulaTime> toNebulaTimeValue(const VariantType &value,
                                                  DateTimeType type = DataTimeType);

    static Status datetimeAddTz(NebulaTime &nebulaTime, const Timezone &timezone);

    static Status timestampAddTz(int64_t &timestamp, const Timezone &timezone);

    static int64_t nebulaTimeToTimestamp(const NebulaTime &nebulaTime);

    static StatusOr<int64_t> strToUTCTimestamp(const std::string &str, const Timezone &timezone);

    static Status UpdateDate(NebulaTime &nebulaTime, int32_t dayCount, bool add);

    static std::string toDateTimeString(const NebulaTime &nebulaTime);
};


class TimeBaseFunc {
public:
    explicit TimeBaseFunc(DateTimeType type = NullType,
                          uint8_t minArity = 0,
                          uint8_t maxArity = 0)
    : type_(type), minArity_(minArity), maxArity_(maxArity) {}

    virtual ~TimeBaseFunc() {}

public:
    virtual Status initNebulaTime(const std::vector<VariantType> &args = {},
                                  const Timezone *timezone = nullptr) {
        if (!checkArgs(args)) {
            LOG(ERROR) << "Wrong number of args";
            return Status::Error("Wrong number of args");
        }

        timezone_ = timezone;
        if (minArity_ == 0 && args.empty()) {
            return Status::OK();
        }
        auto status = TimeCommon::toNebulaTimeValue(args[0], type_);
        if (!status.ok()) {
            LOG(ERROR) << status.status();
            return status.status();
        }
        nebulaTime_ = std::move(status).value();
        args_ = &args;
        return Status::OK();
    }

    virtual StatusOr<VariantType> getResult() {
        auto status = calcDateTime();
        if (!status.ok()) {
            LOG(ERROR) << status;
            return status;
        }
        return TimeCommon::toDateTimeString(nebulaTime_);
    }

    virtual std::string getFuncName() = 0;

    size_t getMinArity() { return  minArity_; }
    size_t getMaxArity() { return  maxArity_; }


    void ptimeToNebula(boost::posix_time::ptime & time, NebulaTime &nebulaTime) {
        nebulaTime.year = time.date().year();
        nebulaTime.month = time.date().month();
        nebulaTime.day = time.date().day();
        nebulaTime.hour = time.time_of_day().hours();
        nebulaTime.minute = time.time_of_day().minutes();
        nebulaTime.second = time.time_of_day().seconds();
        auto totalSeconds = time.time_of_day().total_seconds() * 1000000;
        auto totalMicroSeconds = time.time_of_day().total_microseconds();
        auto microSeconds = totalMicroSeconds - totalSeconds;
        if (microSeconds > 0) {
            nebulaTime.secondPart = microSeconds;
        }
    }

protected:
    virtual Status calcDateTime() {
        return Status::OK();
    }

    bool checkArgs(const std::vector<VariantType> &args) {
        if (args.size() >= minArity_ && args.size() <= maxArity_) {
            return true;
        }
        return false;
    }

protected:
    NebulaTime                                  nebulaTime_;
    DateTimeType                                type_{NullType};
    const std::vector<VariantType>              *args_{nullptr};
    const Timezone                              *timezone_{nullptr};
    size_t                                      minArity_{0};
    size_t                                      maxArity_{0};
};

class CalcTimeFunc : public TimeBaseFunc {
public:
    explicit CalcTimeFunc(bool add) : TimeBaseFunc(DataTimeType, 2, 2), add_(add) {}
    ~CalcTimeFunc() {}

    std::string getFuncName() override {
        return "CalcTimeFunc";
    }

    Status initNebulaTime(const std::vector<VariantType> &args,
                          const Timezone *timezone = nullptr) override;

    Status calcDateTime() override;

private:
    NebulaTime          addNebulaTime_;
    bool                add_{true};
};

class AddDateFunc final : public TimeBaseFunc {
public:
    AddDateFunc() : TimeBaseFunc(DataTimeType, 2, 3) {}
    ~AddDateFunc() {}

    std::string getFuncName() override {
        return "AddDateFunc";
    }
protected:
    Status calcDateTime() override;
};

class AddTimeFunc final : public CalcTimeFunc {
public:
    AddTimeFunc() : CalcTimeFunc(true) {}
    ~AddTimeFunc() {}

    std::string getFuncName() override {
        return "AddTimeFunc";
    }
};

class SubDateFunc final : public TimeBaseFunc {
public:
    SubDateFunc() : TimeBaseFunc(NullType, 2, 2) {}
    ~SubDateFunc() {}

    std::string getFuncName() override {
        return "SubDateFunc";
    }
protected:
    Status calcDateTime() override;
};

class SubTimeFunc final : public CalcTimeFunc {
public:
    SubTimeFunc() : CalcTimeFunc(false) {}
    ~SubTimeFunc() {}

    std::string getFuncName() override {
        return "SubTimeFunc";
    }
};

class ConvertTzFunc final : public TimeBaseFunc {
public:
    ConvertTzFunc() : TimeBaseFunc(NullType, 3, 3) {}
    ~ConvertTzFunc() {}

    std::string getFuncName() override {
        return "ConvertTzFunc";
    }

protected:
    Status calcDateTime() override;
};


class CurrentDateFunc final : public TimeBaseFunc {
public:
    CurrentDateFunc() : TimeBaseFunc(NullType, 0, 0) {}
    ~CurrentDateFunc() {}

    std::string getFuncName() override {
        return "CurrentDateFunc";
    }

protected:
    Status calcDateTime() override;
};

class CurrentTimeFunc final : public TimeBaseFunc {
public:
    CurrentTimeFunc() : TimeBaseFunc(NullType, 0, 0) {}
    ~CurrentTimeFunc() {}

    std::string getFuncName() override {
        return "CurrentTimeFunc";
    }

protected:
    Status calcDateTime() override;
};

class YearFunc final : public TimeBaseFunc {
public:
    YearFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~YearFunc() {}

    std::string getFuncName() override {
        return "YearFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class MonthFunc final : public TimeBaseFunc {
public:
    MonthFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~MonthFunc() {}

    std::string getFuncName() override {
        return "MonthFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class DayFunc final : public TimeBaseFunc {
public:
    DayFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~DayFunc() {}

    std::string getFuncName() override {
        return "DayFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class DayOfMonthFunc final : public TimeBaseFunc {
public:
    DayOfMonthFunc() : TimeBaseFunc(NullType, 1, 1) {}
    ~DayOfMonthFunc() {}

    std::string getFuncName() override {
        return "DayOfMonthFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class DayOfWeekFunc final : public TimeBaseFunc {
public:
    DayOfWeekFunc() : TimeBaseFunc(NullType, 1, 1) {}
    ~DayOfWeekFunc() {}

    std::string getFuncName() override {
        return "DayOfWeekFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class DayOfYearFunc final : public TimeBaseFunc {
public:
    DayOfYearFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~DayOfYearFunc() {}

    std::string getFuncName() override {
        return "DayOfYearFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class HourFunc final : public TimeBaseFunc {
public:
    HourFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~HourFunc() {}

    std::string getFuncName() override {
        return "HourFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class MinuteFunc final : public TimeBaseFunc {
public:
    MinuteFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~MinuteFunc() {}

    std::string getFuncName() override {
        return "MinuteFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class SecondFunc final : public TimeBaseFunc {
public:
    SecondFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~SecondFunc() {}

    std::string getFuncName() override {
        return "SecondFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class TimeFormatFunc final : public TimeBaseFunc {
public:
    TimeFormatFunc() : TimeBaseFunc(NullType, 2, 2) {}
    ~TimeFormatFunc() {}

    std::string getFuncName() override {
        return "TimeFormatFunc";
    }

    StatusOr<VariantType> getResult() override;
};

class TimeToSecFunc final : public TimeBaseFunc {
public:
    TimeToSecFunc() : TimeBaseFunc(DataTimeType, 1, 1) {}
    ~TimeToSecFunc() {}

    std::string getFuncName() override {
        return "TimeToSecFunc";
    }
    StatusOr<VariantType> getResult() override;

protected:
    Status calcDateTime() override;
};

class MakeDateFunc final : public TimeBaseFunc {
public:
    MakeDateFunc() : TimeBaseFunc(DataType, 2, 2) {}
    ~MakeDateFunc() {}

    std::string getFuncName() override {
        return "MakeDateFunc";
    }

protected:
    Status calcDateTime() override;
};

class MakeTimeFunc final : public TimeBaseFunc {
public:
    MakeTimeFunc() : TimeBaseFunc(NullType, 3, 3) {}
    ~MakeTimeFunc() {}

    Status initNebulaTime(const std::vector<VariantType> &args,
                          const Timezone *timezone = nullptr) override;

    std::string getFuncName() override {
        return "MakeTimeFunc";
    }

protected:
    Status calcDateTime() override;
};

class UTCDateFunc final : public TimeBaseFunc {
public:
    UTCDateFunc() : TimeBaseFunc(NullType, 0, 0) {}
    ~UTCDateFunc() {}

    std::string getFuncName() override {
        return "UTCDateFunc";
    }
};

class UTCTimeFunc final : public TimeBaseFunc {
public:
    UTCTimeFunc() : TimeBaseFunc(NullType, 0, 0) {}
    ~UTCTimeFunc() {}

    std::string getFuncName() override {
        return "UTCTimeFunc";
    }
};

class UTCTimestampFunc final : public TimeBaseFunc {
public:
    UTCTimestampFunc() : TimeBaseFunc(NullType, 0, 0) {}
    ~UTCTimestampFunc() {}

    std::string getFuncName() override {
        return "UTCTimestampFunc";
    }
};

class UnixTimestampFunc final : public TimeBaseFunc {
public:
    UnixTimestampFunc() : TimeBaseFunc(DataTimeType, 0, 1) {}
    ~UnixTimestampFunc() {}

    std::string getFuncName() override {
        return "UTCTimestampFunc";
    }
    StatusOr<VariantType> getResult() override;

protected:
    Status calcDateTime() override;

private:
    int64_t  timestamp_{0};
};

class TimestampFormatFunc final : public TimeBaseFunc {
public:
    TimestampFormatFunc() : TimeBaseFunc(TimestampType, 2, 2) {}
    ~TimestampFormatFunc() {}

    std::string getFuncName() override {
        return "TimestampFormatFunc";
    }

    StatusOr<VariantType> getResult() override;
};

namespace time {
static std::unordered_map<std::string,
                          std::function<std::shared_ptr<TimeBaseFunc>()>> timeFuncVec = {
    { "adddate", []() -> auto { return std::make_shared<AddDateFunc>();} },
    { "addtime", []() -> auto { return std::make_shared<AddTimeFunc>();} },
    { "subDate", []() -> auto { return std::make_shared<SubDateFunc>();} },
    { "subtime", []() -> auto { return std::make_shared<SubTimeFunc>();} },
    { "converttz", []() -> auto { return std::make_shared<ConvertTzFunc>();} },
    { "currentdate", []() -> auto { return std::make_shared<CurrentDateFunc>();} },
    { "currenttime", []() -> auto { return std::make_shared<CurrentTimeFunc>();} },
    { "year", []() -> auto { return std::make_shared<YearFunc>();} },
    { "month", []() -> auto { return std::make_shared<MonthFunc>();} },
    { "day", []() -> auto { return std::make_shared<DayFunc>();} },
    { "dayofmonth", []() -> auto { return std::make_shared<DayOfMonthFunc>();} },
    { "dayofweek", []() -> auto { return std::make_shared<DayOfWeekFunc>();} },
    { "dayofyear", []() -> auto { return std::make_shared<DayOfYearFunc>();} },
    { "hour", []() -> auto { return std::make_shared<HourFunc>();} },
    { "minute", []() -> auto { return std::make_shared<MinuteFunc>();} },
    { "second", []() -> auto { return std::make_shared<SecondFunc>();} },
    { "timeformat", []() -> auto { return std::make_shared<TimeFormatFunc>();} },
    { "timetosec", []() -> auto { return std::make_shared<TimeToSecFunc>();} },
    { "utcdate", []() -> auto { return std::make_shared<UTCDateFunc>();} },
    { "utctime", []() -> auto { return std::make_shared<UTCTimeFunc>();} },
    { "utctimestamp", []() -> auto { return std::make_shared<UTCTimestampFunc>();} },
    { "unixtimestamp", []() -> auto { return std::make_shared<UnixTimestampFunc>();} },
    { "timestampformat", []() -> auto { return std::make_shared<TimestampFormatFunc>();}} };

}  // namespace time
}  // namespace nebula

#endif  //  COMMON_FILTER_TIMEFUNCTION_H
