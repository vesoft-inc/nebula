/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_TIMESENTENCE_H
#define PARSER_TIMESENTENCE_H

#include "base/Base.h"
#include "parser/Sentence.h"

namespace nebula {
class TimezoneSentence final : public Sentence {
public:
    enum class TimezoneType : uint8_t {
        kUnknown,
        kGetTimezone,
        kSetTimezone,
    };

public:
    explicit TimezoneSentence(TimezoneType type) {
        kind_ = Kind::kTimezone;
        type_ = type;
    }

    std::string toString() const override;

    void setTimezone(std::string *timezone) {
        timezone_.reset(timezone);
    }

    const std::string* getTimezone() const {
        return timezone_.get();
    }

    TimezoneType getType() const {
        return type_;
    }

private:
    std::unique_ptr<std::string>    timezone_;
    TimezoneType                    type_{TimezoneType::kUnknown};
};
}   // namespace nebula
#endif  // PARSER_TIMESENTENCE_H
