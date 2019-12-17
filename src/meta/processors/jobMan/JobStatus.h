/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBSTATUS_H_
#define META_KVJOBSTATUS_H_

#include <vector>
#include <string>

namespace nebula {
namespace meta {

class JobStatus {
public:
    enum class Status : uint8_t {
        QUEUE           = 0x01,
        RUNNING         = 0x02,
        FINISHED        = 0x03,
        FAILED          = 0x04,
        STOPPED         = 0x05,
        INVALID         = 0x06,
    };

    static std::string toString(Status st);
    static Status toStatus(const std::string& strStatus);
    // static bool laterThan(const std::string& lhs, const std::string& rhs);
    static bool laterThan(Status lhs, Status rhs);
private:
    // static int phaseNumber(const std::string& phase);
    static int phaseNumber(Status st);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBSTATUS_H_
