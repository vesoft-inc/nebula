/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/process/ProcessUtils.h"
#include <signal.h>
#include "common/base/Cord.h"
#include "common/fs/FileUtils.h"

namespace nebula {

Status ProcessUtils::isPidAvailable(pid_t pid) {
    if (pid == ::getpid()) {
        return Status::OK();
    }

    constexpr auto SIG_OK = 0;
    if (::kill(pid, SIG_OK) == 0) {
        return Status::Error("Process `%d' already existed", pid);
    }
    if (errno == EPERM) {
        return Status::Error("Process `%d' already existed but denied to access", pid);
    }
    if (errno != ESRCH) {
        return Status::Error("Uknown error: `%s'", ::strerror(errno));
    }
    return Status::OK();
}


Status ProcessUtils::isPidAvailable(const std::string &pidFile) {
    // Test existence and readability
    if (::access(pidFile.c_str(), R_OK) == -1) {
        if (errno == ENOENT) {
            return Status::OK();
        } else {
            return Status::Error("%s: %s", pidFile.c_str(), ::strerror(errno));
        }
    }
    // Pidfile is readable
    static const std::regex pattern("([0-9]+)");
    fs::FileUtils::FileLineIterator iter(pidFile, &pattern);
    if (!iter.valid()) {
        // Pidfile is readable but has no valid pid
        return Status::OK();
    }
    // Now we have a valid pid
    return isPidAvailable(folly::to<uint32_t>(iter.matched()[1].str()));
}


Status ProcessUtils::makePidFile(const std::string &pidFile, pid_t pid) {
    if (pidFile.empty()) {
        return Status::Error("Path to the pid file is empty");
    }
    // Create hosting directory if not exists
    auto dirname = fs::FileUtils::dirname(pidFile.c_str());
    if (!fs::FileUtils::makeDir(dirname)) {
        return Status::Error("Failed to create: `%s'", dirname.c_str());
    }
    // Open or create pid file
    auto *file = ::fopen(pidFile.c_str(), "w");
    if (file == nullptr) {
        return Status::Error("Open or create `%s': %s", pidFile.c_str(), ::strerror(errno));
    }

    if (pid == 0) {
        pid = ::getpid();
    }

    ::fprintf(file, "%d\n", pid);
    ::fflush(file);
    ::fclose(file);

    return Status::OK();
}


Status ProcessUtils::daemonize(const std::string &pidFile) {
    auto pid = ::fork();
    if (pid == -1) {
        return Status::Error("fork: %s", ::strerror(errno));
    }
    if (pid > 0) {  // parent process
        ::exit(0);
    }

    // Make the child process as the session leader and detach with the controlling terminal
    ::setsid();
    // Set `/dev/null' as standard input
    auto fd = ::open("/dev/null", O_RDWR);
    ::dup2(fd, 0);
    ::close(fd);

    return makePidFile(pidFile);
}


StatusOr<std::string> ProcessUtils::getExePath(pid_t pid) {
    if (pid == 0) {
        pid = ::getpid();
    }
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "/proc/%d/exe", pid);
    return fs::FileUtils::readLink(path);
}


StatusOr<std::string> ProcessUtils::getExeCWD(pid_t pid) {
    if (pid == 0) {
        pid = ::getpid();
    }
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "/proc/%u/cwd", pid);
    return fs::FileUtils::readLink(path);
}


StatusOr<std::string> ProcessUtils::getProcessName(pid_t pid) {
    if (pid == 0) {
        pid = ::getpid();
    }
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "/proc/%d/comm", pid);
    fs::FileUtils::FileLineIterator iter(path);
    if (!iter.valid()) {
        return iter.status();
    }
    return iter.entry();
}


pid_t ProcessUtils::maxPid() {
    static const std::regex pattern("([0-9]+)");
    fs::FileUtils::FileLineIterator iter("/proc/sys/kernel/pid_max", &pattern);
    CHECK(iter.valid());
    return folly::to<uint32_t>(iter.matched()[1].str());
}


StatusOr<std::string> ProcessUtils::runCommand(const char* command) {
    FILE* f = popen(command, "re");
    if (f == nullptr) {
        return Status::Error("Failed to execute the command \"%s\"", command);
    }

    Cord out;
    char buf[1024];
    size_t len;
    do {
        len = fread(buf, 1, 1024, f);
        if (len > 0) {
            out.write(buf, len);
        }
    } while (len == 1024);

    if (ferror(f)) {
        // Something is wrong
        fclose(f);
        return Status::Error("Failed to read the output of the command");
    }

    pclose(f);
    return out.str();
}

}   // namespace nebula
