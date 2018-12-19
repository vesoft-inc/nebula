/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_PROCESSUTILS_H_
#define COMMON_PROCESSUTILS_H_

#include "base/Base.h"
#include "base/StatusOr.h"

namespace nebula {

class ProcessUtils final {
public:
    ProcessUtils() = delete;

    // Tell if there is no existing process who has the same pid as `pid'.
    static Status isPidAvailable(uint32_t pid);
    /**
     * Like isPidAvailable(uint32_t), but use a file which contains a pid as input.
     * Returns Status::OK if:
     *      The pidFile does not exist.
     *      The pidFile is readable but has no valid pid.
     *      The pidFile contains a valid pid and no such process exists.
     */
    static Status isPidAvailable(const std::string &pidFile);
    static Status makePidFile(const std::string &path, uint32_t pid = 0);
    /**
     * Get the absolute path to the target process's executable.
     * Use the current process if pid == 0.
     */
    static StatusOr<std::string> getExePath(uint32_t pid = 0);
    /**
     * Get the absolute path to the current working directory of the target process.
     * Use the current process if pid == 0.
     */
    static StatusOr<std::string> getExeCWD(uint32_t pid = 0);
    /**
     * Get the name of the target process.
     * Use the current process if pid == 0.
     */
    static StatusOr<std::string> getProcessName(uint32_t pid = 0);
    /**
     * Get the maximum pid of the system.
     */
    static uint32_t maxPid();
};

}   // namespace nebula

#endif  // COMMON_PROCESSUTILS_H_
