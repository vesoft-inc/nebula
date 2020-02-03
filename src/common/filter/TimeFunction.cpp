/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "TimeFunction.h"
#include "time/WallClock.h"
#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/gregorian/gregorian_io.hpp>
#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace nebula {

const uint8_t daysOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

constexpr int64_t maxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;

using boost::posix_time::ptime;
using boost::gregorian::date;
using boost::gregorian::days;
using boost::gregorian::weeks;
using boost::posix_time::time_facet;
using boost::posix_time::hours;
using boost::posix_time::minutes;
using boost::posix_time::seconds;
using boost::posix_time::millisec;

// get timezone struct
StatusOr<Timezone> TimeCommon::getTimezone(const std::string &timezoneStr) {
    if (timezoneStr.length() != 6) {
        return Status::Error("Invalid timezone type");
    }

    if ((timezoneStr[0] != '+' && timezoneStr[0] != '-') ||
        (timezoneStr[1] > '1' && timezoneStr[1] < '0') ||
        (timezoneStr[2] > '9' && timezoneStr[2] < '0') ||
        (timezoneStr[4] > '1' && timezoneStr[4] < '0') ||
        (timezoneStr[5] > '9' && timezoneStr[4] < '0') ||
        timezoneStr[3] != ':') {
        return Status::Error("Invalid timezone type");
    }

    auto hour = (timezoneStr[1] - '0') * 10 + (timezoneStr[2] - '0');
    auto minute = (timezoneStr[4] - '0') * 10 + (timezoneStr[5] - '0');

    if (hour > 13 || minute > 59 || (hour == 13 && minute != 0)) {
        return Status::Error("Invalid timezone type");
    }
    if (timezoneStr[0] != '-' && hour == 13) {
        return Status::Error("Invalid timezone type");
    }

    Timezone timezone;
    timezone.eastern = timezoneStr[0] == '-' ? '-' : '+';
    timezone.hour = hour;
    timezone.minute = minute;
    return timezone;
}

// get current datetime
NebulaTime TimeCommon::getUTCTime(bool decimals) {
    time_t seconds;
    int64_t microSeconds;
    NebulaTime result;
    result.timeType = DataTimeType;
    if (decimals) {
        auto time = time::WallClock::fastNowInMicroSec();
        microSeconds = time % 1000000;
        seconds = time / 1000000;
        result.secondPart = microSeconds;
    } else {
        seconds = time::WallClock::fastNowInSec();
    }

    struct tm utcTime;
    if (0 != gmtime_r(&seconds, &utcTime)) {
        result.year = utcTime.tm_year + 1900;
        result.month = utcTime.tm_mon + 1;
        result.day = utcTime.tm_mday;
        result.hour = utcTime.tm_hour;
        result.minute = utcTime.tm_min;
        result.second = utcTime.tm_sec;
    }
    return result;
}

// return true if value is Ok
bool TimeCommon::checkDatetime(const NebulaTime &nTime) {
    if (nTime.year > 9999U || nTime.month > 12U ||
        nTime.minute > 59U || nTime.second > 59U ||
        nTime.secondPart > 999999U || nTime.hour > 23U ||
        nTime.timestamp > maxTimestamp) {
        return false;
    }
    // only has time or year
    if (nTime.month == 0) {
        return true;
    }
    if (nTime.month != 2) {
        if (nTime.day > daysOfMonth[nTime.month - 1]) {
            return false;
        }
    } else {
        if (isLeapyear(nTime.year) && nTime.day > 29U) {
            return false;
        } else if (!isLeapyear(nTime.year) && nTime.day > 28U) {
            return false;
        }
    }

    return true;
}

StatusOr<NebulaTime> TimeCommon::getDate(int64_t date) {
    NebulaTime result;
    // make it as year
    if (date < 10000) {
        result.year = date;
        result.timeType = DataType;
        return result;
    }

    result.day = date % 100;
    result.month = date / 100 % 100;
    result.year = date / 10000;
    result.timeType = DataType;
    return result;
}

StatusOr<NebulaTime> TimeCommon::getTime(int64_t time) {
    NebulaTime result;
    result.second = time % 100;
    result.minute = time / 100 % 100;
    result.hour = time / 10000 % 100;
    result.timeType = TimeType;
    return result;
}

StatusOr<NebulaTime> TimeCommon::getDateTime(int64_t time) {
    NebulaTime result;
    int64_t timePart = time % 1000000;
    int64_t dataPart = time / 1000000;

    result.second = timePart % 100;
    result.minute = timePart / 100 % 100;
    result.hour = timePart / 10000;
    result.day = dataPart % 100;
    result.month = dataPart / 100 % 100;
    result.year = dataPart / 10000;
    result.timeType = TimeType;
    return result;
}

StatusOr<NebulaTime> TimeCommon::getTimestamp(int64_t time) {
    NebulaTime result;
    result.timestamp = time;
    result.timeType = TimestampType;
    return result;
}

StatusOr<NebulaTime> TimeCommon::isDate(const std::string &value) {
    if (value.find('-') == std::string::npos ||
        value.find(':') != std::string::npos) {
        return Status::Error("without \'-\' or has \':\'");
    }
    static const std::regex reg("^([1-9]\\d{3})-"
                                "(0[1-9]|1[0-2]|\\d)-"
                                "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+");
    std::smatch result;
    if (!std::regex_match(value, result, reg)) {
        return Status::Error("Invalid date type");
    }
    NebulaTime nTime;
    nTime.year = atoi(result[1].str().c_str());
    nTime.month = atoi(result[2].str().c_str());
    nTime.day = atoi(result[3].str().c_str());
    nTime.timeType = DataType;
    return nTime;
}

StatusOr<NebulaTime> TimeCommon::isTime(const std::string &value) {
    if (value.find('-') != std::string::npos ||
        value.find(':') == std::string::npos) {
        return Status::Error("without \':\' or has \'-\'");
    }
    NebulaTime nTime;
    std::string temp = value;
    auto pos = temp.find('.');
    if (pos != std::string::npos) {
        std::string secondPart = temp.substr(pos + 1);
        try {
            auto resultInt = folly::to<int64_t>(secondPart);
            if (resultInt > 999999) {
                return Status::Error("SecondPart out of rang 999999");
            }
            nTime.secondPart = resultInt;
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to cast string to int: " << e.what();
            return Status::Error("Failed to cast string to int: %s", e.what());
        }
        temp = temp.substr(0, pos);
    }
    // TODO: regex is inefficient, need to modify
    static const std::regex reg("^(20|21|22|23|[0-1]\\d|\\d):"
                                "([0-5]\\d|\\d):"
                                "([0-5]\\d|\\d)$");
    std::smatch result;
    if (!std::regex_match(temp, result, reg)) {
        return Status::Error("Invalid time type");
    }

    nTime.hour = atoi(result[1].str().c_str());
    nTime.minute = atoi(result[2].str().c_str());
    nTime.second = atoi(result[3].str().c_str());
    nTime.timeType = TimeType;
    return nTime;
}

StatusOr<NebulaTime> TimeCommon::isDateTime(const std::string &value) {
    if (value.find("-") == std::string::npos ||
        value.find(":") == std::string::npos) {
        return Status::Error("without \':\' or \'-\'");
    }
    NebulaTime nTime;
    std::string temp = value;
    auto pos = value.find(".");
    if (pos != std::string::npos) {
        std::string secondPart = temp.substr(pos + 1);
        try {
            auto resultInt = folly::to<int64_t>(secondPart);
            if (resultInt > 999999) {
                return Status::Error("SecondPart out of rang 999999");
            }
            nTime.secondPart = resultInt;
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to cast string to int: " << e.what();
            return Status::Error("Failed to cast string to int: %s", e.what());
        }
        temp = temp.substr(0, pos);
    }
    // TODO: regex is inefficient, need to modify
    static const std::regex reg("^([1-9]\\d{3})-"
                                "(0[1-9]|1[0-2]|\\d)-"
                                "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+"
                                "(20|21|22|23|[0-1]\\d|\\d):"
                                "([0-5]\\d|\\d):"
                                "([0-5]\\d|\\d)$");
    std::smatch result;
    if (!std::regex_match(temp, result, reg)) {
        return Status::Error("Invalid time type");
    }

    nTime.year = atoi(result[1].str().c_str());
    nTime.month = atoi(result[2].str().c_str());
    nTime.day = atoi(result[3].str().c_str());
    nTime.hour = atoi(result[4].str().c_str());
    nTime.minute = atoi(result[5].str().c_str());
    nTime.second = atoi(result[6].str().c_str());
    nTime.timeType = DataTimeType;
    return nTime;
}

bool TimeCommon::isLeapyear(int32_t year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

// covert 20191201101000 to NebulaTime
StatusOr<NebulaTime> TimeCommon::intToNebulaTime(int64_t value, DateTimeType type) {
    switch (type) {
        case DataType:
            return getDate(value);
        case TimeType:
            return getTime(value);
        case DataTimeType:
            return getDateTime(value);
        case TimestampType:
            return getTimestamp(value);
        default:
            return Status::Error("Invalid type: %d", type);
    }
}

// covert "20191201101000" to NebulaTime
StatusOr<NebulaTime> TimeCommon::strToNebulaTime1(const std::string &value, DateTimeType type) {
    int64_t intValue = 0;
    try {
        intValue = folly::to<int64_t>(value);
    } catch (std::exception &e) {
        LOG(ERROR) << "Failed to cast string to int: " << e.what();
        return Status::Error("Failed to cast string to int: %s", e.what());
    }

    return intToNebulaTime(intValue, type);
}

// covert "2019-12-01 10:10:00" to NebulaTime
StatusOr<NebulaTime> TimeCommon::strToNebulaTime2(const std::string &value, DateTimeType type) {
    NebulaTime result;
    switch (type) {
        case DataType:
            return isDate(value);
        case TimeType:
            return isTime(value);
        case DataTimeType:
            return isDateTime(value);
        default: {
            auto status = isDate(value);
            if (status.ok()) {
                return status.value();
            }
            status = isTime(value);
            if (status.ok()) {
                return status.value();
            }
            status = isDateTime(value);
            if (status.ok()) {
                return status.value();
            }
            return Status::Error("Invalid type: %d", type);
        }
    }
}

// covert OptVariantType to NebulaTime
StatusOr<NebulaTime> TimeCommon::toNebulaTimeValue(const VariantType &value, DateTimeType type) {
    StatusOr<NebulaTime> status;
    switch (value.which()) {
        case VAR_INT64:
            status = intToNebulaTime(boost::get<int64_t>(value), type);
            break;
        case VAR_STR: {
            std::string temp = boost::get<std::string>(value);
            if (temp.find("-") != std::string::npos ||
                temp.find(":") != std::string::npos) {
                status = strToNebulaTime2(temp, type);
            } else {
                status = strToNebulaTime1(temp, type);
            }
            break;
        }
        default:
            return Status::Error("Error type: %d", value.which());
    }
    if (!status.ok()) {
        return status.status();
    }
    auto resultValue = status.value();
    if (!checkDatetime(resultValue)) {
        return Status::Error("Wrong time format");
    }
    return resultValue;
}

Status TimeCommon::UpdateDate(NebulaTime &nebulaTime, int32_t dayCount, bool add) {
    boost::gregorian::date newDate;
    try {
        if (add) {
            newDate = boost::gregorian::date(nebulaTime.year, nebulaTime.month, nebulaTime.day) +
                      boost::gregorian::days(dayCount);
        } else {
            newDate = boost::gregorian::date(nebulaTime.year, nebulaTime.month, nebulaTime.day) -
                      boost::gregorian::days(dayCount);
        }
    } catch(std::exception& e) {
        LOG(ERROR) << "Error bad date: " << e.what();
        return Status::Error("Error bad date");
    }
    nebulaTime.year = newDate.year();
    nebulaTime.month = newDate.month();
    nebulaTime.day = newDate.day();
    return Status::OK();
}

Status TimeCommon::datetimeAddTz(NebulaTime &nebulaTime, const Timezone &timezone) {
    bool add = true;
    bool updateDate = false;
    int32_t dayCount = 0;
    if (timezone.eastern == '-') {
        if (nebulaTime.minute > timezone.minute) {
            nebulaTime.minute -= timezone.minute;
        } else {
            nebulaTime.hour--;
            nebulaTime.minute = nebulaTime.minute + 60 - timezone.minute;
        }
        if (nebulaTime.hour > timezone.hour) {
            nebulaTime.hour -= timezone.hour;
        } else {
            updateDate = true;
            add = false;
            if (nebulaTime.hour + 24 < timezone.hour) {
                dayCount = 2;
                nebulaTime.hour = nebulaTime.hour + 48 - timezone.hour;
            } else {
                dayCount = 1;
                nebulaTime.hour = nebulaTime.hour + 24 - timezone.hour;
            }
        }
    } else {
        if (nebulaTime.minute + timezone.minute < 60) {
            nebulaTime.minute += timezone.minute;
        } else {
            updateDate = true;
            nebulaTime.hour++;
            nebulaTime.minute = nebulaTime.minute + timezone.minute - 60;
        }
        if (nebulaTime.hour + timezone.hour < 24) {
            nebulaTime.hour += timezone.hour;
        } else if (nebulaTime.hour + timezone.hour >= 48) {
            updateDate = true;
            dayCount = 2;
            nebulaTime.hour = nebulaTime.hour + timezone.hour - 48;
        } else {
            updateDate = true;
            dayCount = 1;
            nebulaTime.hour = nebulaTime.hour + timezone.hour - 24;
        }
    }

    if (updateDate) {
        auto updateStatus = TimeCommon::UpdateDate(nebulaTime, dayCount, add);
        if (!updateStatus.ok()) {
            return updateStatus;
        }
    }
    return Status::OK();
}

Status TimeCommon::timestampAddTz(int64_t &timestamp, const Timezone &timezone) {
    uint32_t seconds = timezone.hour * 3600 + timezone.minute * 60;
    if (timezone.eastern == '-') {
        if (timestamp < seconds) {
            return Status::Error("Out of timestamp");
        }
        timestamp -= seconds;
    } else {
        if (timestamp + seconds > maxTimestamp) {
            return Status::Error("Out of timestamp");
        }
        timestamp += seconds;
    }
    return Status::OK();
}

int64_t TimeCommon::nebulaTimeToTimestamp(const NebulaTime &nebulaTime) {
    ptime time(date(nebulaTime.year, nebulaTime.month, nebulaTime.day),
               hours(nebulaTime.hour) + minutes(nebulaTime.minute) +
               seconds(nebulaTime.second) + millisec(nebulaTime.secondPart));
    int64_t timestamp = boost::posix_time::to_time_t(time);

    return timestamp;
}

StatusOr<int64_t> TimeCommon::strToUTCTimestamp(const std::string &str, const Timezone &timezone) {
    auto timeStatus = isDateTime(str);
    if (!timeStatus.ok()) {
        return timeStatus.status();
    }
    NebulaTime nebulaTime = std::move(timeStatus).value();
    LOG(INFO) << "nebulaTime" << toDateTimeString(nebulaTime);
    auto status = datetimeAddTz(nebulaTime, timezone);
    if (!status.ok()) {
        return status;
    }
    return nebulaTimeToTimestamp(nebulaTime);
}

std::string TimeCommon::toDateTimeString(const NebulaTime &nebulaTime) {
    switch (nebulaTime.timeType) {
        case DataTimeType:
            if (nebulaTime.secondPart > 0) {
                return folly::stringPrintf("%4d-%02d-%02d %02d:%02d:%02d.%d",
                                           nebulaTime.year, nebulaTime.month,
                                           nebulaTime.day, nebulaTime.hour,
                                           nebulaTime.minute, nebulaTime.second,
                                           nebulaTime.secondPart);
            }
            return folly::stringPrintf("%4d-%02d-%02d %02d:%02d:%02d",
                                       nebulaTime.year, nebulaTime.month, nebulaTime.day,
                                       nebulaTime.hour, nebulaTime.minute, nebulaTime.second);
        case DataType:
            return folly::stringPrintf("%4d-%02d-%02d",
                                       nebulaTime.year, nebulaTime.month, nebulaTime.day);
        case TimeType:
            return folly::stringPrintf("%2d:%02d:%02d",
                                       nebulaTime.hour, nebulaTime.minute, nebulaTime.second);
        default:
            return "NULL";
    }
}

Status AddDateFunc::calcDateTime() {
    if (args_->size() == minArity_) {
        if ((*args_)[1].which() != VAR_INT64) {
            return Status::Error("Wrong days type");
        }

        return TimeCommon::UpdateDate(nebulaTime_, boost::get<int64_t>((*args_)[1]), true);
    } else {
        if ((*args_)[1].which() != VAR_INT64 || (*args_)[2].which() != VAR_INT64) {
            return Status::Error("Wrong days type");
        }

        auto count = boost::get<int64_t>((*args_)[1]);
        IntervalType intervalType = static_cast<IntervalType>(boost::get<int64_t>((*args_)[2]));
        if (intervalType == MONTH) {
            if (nebulaTime_.month + count > 12) {
                if (nebulaTime_.year + 1 > 9999) {
                    return Status::Error("Year is out of rang");
                }
                nebulaTime_.year++;
                nebulaTime_.month = nebulaTime_.month + count - 12;
            } else {
                nebulaTime_.month += count;
            }
        } else if (intervalType == YEAR) {
            if (nebulaTime_.year + count > 9999) {
                return Status::Error("Year is out of rang");
            } else {
                nebulaTime_.year += count;
            }

        } else {
            ptime time(date(nebulaTime_.year, nebulaTime_.month, nebulaTime_.day),
                            hours(nebulaTime_.hour) + minutes(nebulaTime_.minute) +
                            seconds(nebulaTime_.second) + millisec(nebulaTime_.secondPart));
            switch (intervalType) {
                case MICROSECOND:
                    time = time + millisec(count);
                    break;
                case SECOND:
                    time = time + seconds(count);
                    break;
                case MINUTE:
                    time = time + minutes(count);
                    break;
                case HOUR:
                    time = time + hours(count);
                    break;
                case DAY:
                    time = time + days(count);
                    break;
                case WEEK:
                    time = time + weeks(count);
                    break;
                default:
                    return Status::Error("Types that cannot be handled");
            }
            ptimeToNebula(time, nebulaTime_);
        }
        return Status::OK();
    }
}

Status CalcTimeFunc::initNebulaTime(const std::vector<VariantType> &args,
                                    const Timezone *timezone) {
    if (!checkArgs(args)) {
        LOG(ERROR) << "Wrong number args";
        return Status::Error("Wrong number args");
    }
    auto status = TimeCommon::toNebulaTimeValue(args[0], DataTimeType);
    if (!status.ok()) {
        LOG(ERROR) << status.status();
        return status.status();
    }
    nebulaTime_ = std::move(status).value();
    auto addStatus = TimeCommon::toNebulaTimeValue(args[1], TimeType);
    if (!addStatus.ok()) {
        LOG(ERROR) << addStatus.status();
        return addStatus.status();
    }
    addNebulaTime_ = std::move(addStatus).value();
    if (addNebulaTime_.year != 0 || addNebulaTime_.month != 0 || addNebulaTime_.day != 0) {
        LOG(ERROR) << "Wrong type";
        return Status::Error("Wrong type");
    }
    timezone_ = timezone;
    args_ = &args;
    return Status::OK();
}

Status CalcTimeFunc::calcDateTime() {
    try {
        date date1(nebulaTime_.year, nebulaTime_.month, nebulaTime_.day);
        ptime time1(date1, hours(nebulaTime_.hour) + minutes(nebulaTime_.minute) +
                           seconds(nebulaTime_.second) + millisec(nebulaTime_.secondPart));
        ptime time2;
        if (add_) {
            time2 = time1 + hours(addNebulaTime_.hour) + minutes(addNebulaTime_.minute) +
                    seconds(addNebulaTime_.second) + millisec(addNebulaTime_.secondPart);
        } else {
            time2 = time1 - hours(addNebulaTime_.hour) - minutes(addNebulaTime_.minute) -
                    seconds(addNebulaTime_.second) - millisec(addNebulaTime_.secondPart);
        }

        ptimeToNebula(time2, nebulaTime_);
        return Status::OK();
    } catch(std::exception& e) {
        LOG(ERROR) << "  Exception: " <<  e.what();
        return Status::Error(folly::stringPrintf("Error : %s", e.what()));
    }
}

Status SubDateFunc::calcDateTime() {
    if ((*args_)[1].which() != VAR_INT64) {
        return Status::Error("Wrong days type");
    }

    return TimeCommon::UpdateDate(nebulaTime_, boost::get<int64_t>((*args_)[1]), false);
}

Status ConvertTzFunc::calcDateTime() {
    if ((*args_)[1].which() != VAR_STR && (*args_)[2].which() != VAR_STR) {
        return Status::Error("Wrong timezone type");
    }
    auto statusTz = TimeCommon::getTimezone(boost::get<std::string>((*args_)[1]));
    if (!statusTz.ok()) {
        LOG(ERROR) << statusTz.status();
        return statusTz.status();
    }
    auto timezone1 = statusTz.value();

    statusTz = TimeCommon::getTimezone(boost::get<std::string>((*args_)[2]));
    if (!statusTz.ok()) {
        return statusTz.status();
    }
    auto timezone2 = statusTz.value();

    Timezone timezone;
    if (timezone1.eastern != timezone2.eastern) {
        timezone.minute = timezone1.minute + timezone2.minute;
        if (timezone.minute >= 60) {
            timezone.minute -= 60;
            timezone.hour++;
        }
        timezone.hour += timezone1.hour + timezone2.hour;
        if (timezone1.eastern == '+') {
            timezone.eastern = '-';
        } else {
            timezone.eastern = '+';
        }
    } else {
        timezone.minute = timezone2.minute - timezone1.minute;
        timezone.hour = timezone2.hour - timezone1.hour;
        if (timezone1.eastern == '+') {
            timezone.eastern = '+';
        } else {
            timezone.eastern = '-';
        }
    }

    auto status = TimeCommon::datetimeAddTz(nebulaTime_, timezone);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status CurrentDateFunc::calcDateTime() {
    if (timezone_ == nullptr) {
        return Status::Error("Get timezone failed");
    }

    nebulaTime_ = TimeCommon::getUTCTime();
    if (timezone_->eastern == '-') {
        if (nebulaTime_.hour < timezone_->hour) {
            auto updateStatus = TimeCommon::UpdateDate(nebulaTime_, 1, false);
            if (!updateStatus.ok()) {
                return updateStatus;
            }
        }
    } else {
        if (nebulaTime_.hour + timezone_->hour >= 24) {
            auto updateStatus = TimeCommon::UpdateDate(nebulaTime_, 1, true);
            if (!updateStatus.ok()) {
                return updateStatus;
            }
        }
    }
    return Status::OK();
}

Status CurrentTimeFunc::calcDateTime() {
    if (timezone_ == nullptr) {
        return Status::Error("Get timezone failed");
    }

    nebulaTime_ = TimeCommon::getUTCTime();
    if (timezone_->eastern == '-') {
        if (nebulaTime_.minute > timezone_->minute) {
            nebulaTime_.minute -= timezone_->minute;
        } else {
            nebulaTime_.hour--;
            nebulaTime_.minute = nebulaTime_.minute + 60 - timezone_->minute;
        }
        if (nebulaTime_.hour > timezone_->hour) {
            nebulaTime_.hour -= timezone_->hour;
        } else {
            nebulaTime_.hour = nebulaTime_.hour + 24 - timezone_->hour;
        }
    } else {
        if (nebulaTime_.minute + timezone_->minute < 60) {
            nebulaTime_.minute += timezone_->minute;
        } else {
            nebulaTime_.hour++;
            nebulaTime_.minute = nebulaTime_.minute + timezone_->minute - 60;
        }
        if (nebulaTime_.hour + timezone_->hour < 24) {
            nebulaTime_.hour += timezone_->hour;
        } else {
            nebulaTime_.hour = nebulaTime_.hour + timezone_->hour - 24;
        }
    }
    return Status::OK();
}

StatusOr<VariantType> YearFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.year);
}

StatusOr<VariantType> MonthFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.month);
}


StatusOr<VariantType> DayFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.day);
}

StatusOr<VariantType> DayOfMonthFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.day);
}

StatusOr<VariantType> DayOfWeekFunc::getResult() {
    boost::gregorian::date inputDate(nebulaTime_.year, nebulaTime_.month, nebulaTime_.day);
    return boost::lexical_cast<std::string>(inputDate.day_of_week());
}

StatusOr<VariantType> DayOfYearFunc::getResult() {
    boost::gregorian::date inputDate(nebulaTime_.year, nebulaTime_.month, nebulaTime_.day);
    return boost::lexical_cast<int64_t>(inputDate.day_of_year());
}

StatusOr<VariantType> HourFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.hour);
}

StatusOr<VariantType> MinuteFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.minute);
}

StatusOr<VariantType> SecondFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.second);
}

StatusOr<VariantType> TimeFormatFunc::getResult() {
    if ((*args_)[1].which() != VAR_STR) {
        return Status::Error("Error format type");
    }

    try {
        date d(nebulaTime_.year, nebulaTime_.month, nebulaTime_.day);
        ptime time(d, hours(nebulaTime_.hour) + minutes(nebulaTime_.minute) +
                   seconds(nebulaTime_.second) + millisec(nebulaTime_.secondPart));
        time_facet *timeFacet = new time_facet(
            boost::get<std::string>((*args_)[1]).c_str());

        std::stringstream stream;
        stream.imbue(std::locale(stream.getloc(), timeFacet));
        stream << time;
        return stream.str();
    } catch(std::exception& e) {
        LOG(ERROR) << "Fromat Exception: " <<  e.what();
        return Status::Error(folly::stringPrintf("Error : %s", e.what()));
    }
}

StatusOr<VariantType> TimeToSecFunc::getResult() {
    return static_cast<int64_t>(nebulaTime_.hour * 3600 +
        nebulaTime_.minute * 60 + nebulaTime_.second);
}

Status TimeToSecFunc::calcDateTime() {
    return Status::Error("Does not support");
}

Status MakeDateFunc::calcDateTime() {
    if ((*args_)[1].which() != VAR_INT64) {
        return Status::Error("Wrong dayofyear type: %d", (*args_)[1].which());
    }
    nebulaTime_.month = 1;
    nebulaTime_.day = 1;
    auto status = TimeCommon::UpdateDate(nebulaTime_, boost::get<int64_t>((*args_)[1]), true);
    if (!status.ok()) {
        return status;
    }
    // Minus the initial value
    if (nebulaTime_.month == 1) {
        status = TimeCommon::UpdateDate(nebulaTime_, 1, false);
    } else {
        status = TimeCommon::UpdateDate(nebulaTime_, 31, false);
    }

    return status;
}

Status MakeTimeFunc::initNebulaTime(const std::vector<VariantType> &args, const Timezone*) {
    if (!checkArgs(args)) {
        return Status::Error("Error args number");
    }
    if (args[0].which() != VAR_INT64 ||
            args[1].which() != VAR_INT64 ||
            args[2].which() != VAR_INT64) {
        return Status::Error("Wrong time type");
    }
    args_ = &args;
    return Status::OK();
}

Status MakeTimeFunc::calcDateTime() {
    nebulaTime_.hour = boost::get<int64_t>((*args_)[0]);
    nebulaTime_.minute = boost::get<int64_t>((*args_)[1]);
    nebulaTime_.second = boost::get<int64_t>((*args_)[2]);
    nebulaTime_.timeType = TimeType;

    if (nebulaTime_.hour > 23 || nebulaTime_.minute > 59 || nebulaTime_.second > 59) {
        return Status::Error("Wrong time type");
    }
    return Status::OK();
}

StatusOr<VariantType> UnixTimestampFunc::getResult() {
    auto status = calcDateTime();
    if (!status.ok()) {
        return status;
    }
    return timestamp_;
}

Status UnixTimestampFunc::calcDateTime() {
    if (args_ != nullptr) {
        timestamp_ = TimeCommon::nebulaTimeToTimestamp(nebulaTime_);
    } else {
        timestamp_ = time::WallClock::fastNowInSec();
    }
    return Status::OK();
}

StatusOr<VariantType> TimestampFormatFunc::getResult() {
    try {
        ptime time = boost::posix_time::from_time_t(nebulaTime_.timestamp);
        time_facet *timeFacet = new time_facet(
                boost::get<std::string>((*args_)[1]).c_str());

        std::stringstream stream;
        stream.imbue(std::locale(stream.getloc(), timeFacet));
        stream << time;
        return stream.str();
    } catch(std::exception& e) {
        LOG(ERROR) << "Fromat Exception: " <<  e.what();
        return Status::Error(folly::stringPrintf("Error : %s", e.what()));
    }
}
}  // namespace nebula
//  COMMON_FILTER_TIMEFUNCTION_H
