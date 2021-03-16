/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "process/ProcessUtils.h"
#include <signal.h>
#include "base/Cord.h"
#include "fs/FileUtils.h"
#include "folly/Subprocess.h"
#include <boost/interprocess/anonymous_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <future>

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

struct BgRun {
    enum {
        NumItems = 10,
        LineSize = 4096,
    };

    struct BgRunReq {
        size_t idx{};
        char content[LineSize]{};
    };

    struct BgRunReqQueue {
        BgRunReqQueue(): mutex(1), nempty(NumItems), nstored(0) { }

        boost::interprocess::interprocess_semaphore
            mutex, nempty, nstored;

        size_t idxBegin = 0;
        size_t idxEnd = 0;
        BgRunReq items[NumItems]{};

        BgRunReq wait() {
            nstored.wait();
            mutex.wait();
            CHECK(idxBegin <= idxEnd);
            BgRunReq item = items[idxBegin++ % NumItems];
            mutex.post();
            nempty.post();
            return item;
        }

        size_t post(const std::string& cmd) {
            nempty.wait();
            mutex.wait();
            CHECK(idxBegin <= idxEnd);
            auto idx = idxEnd++;
            BgRunReq &req = items[idx % NumItems];
            req.idx = idx;
            snprintf(req.content, LineSize, "%s", cmd.c_str());
            mutex.post();
            nstored.post();
            return idx;
        }
    };

    struct BgRunResp {
        size_t idx{};
        bool success{};
        char content[LineSize]{};
    };

    struct BgRunRespQueue {
        BgRunRespQueue(): mutex(1), nempty(NumItems), nstored(0) { }

        boost::interprocess::interprocess_semaphore
            mutex, nempty, nstored;

        size_t idxBegin = 0;
        size_t idxEnd = 0;
        BgRunResp items[NumItems]{};

        BgRunResp wait() {
            nstored.wait();
            mutex.wait();
            CHECK(idxBegin <= idxEnd);
            BgRunResp item = items[idxBegin++ % NumItems];
            mutex.post();
            nempty.post();
            return item;
        }

        void post(BgRunResp item) {
            nempty.wait();
            mutex.wait();
            CHECK(idxBegin <= idxEnd);
            items[idxEnd % NumItems] = item;
            idxEnd++;
            mutex.post();
            nstored.post();
        }
    };

    BgRunReqQueue reqQueue;
    BgRunRespQueue respQueue;

    std::unordered_map<size_t, std::promise<std::string>> results{};
    std::mutex mutex;

    BgRun() {
        std::thread([this] {
            while (true) {
                BgRunResp resp = respQueue.wait();
                std::unique_lock<std::mutex> l(mutex);
                auto idx = resp.idx;
                if (results.find(idx) == results.end()) {
                    std::promise<std::string> pro;
                    if (resp.success) {
                        pro.set_value(std::string(resp.content));
                    } else {
                        pro.set_exception(std::make_exception_ptr(
                            std::runtime_error(resp.content)));
                    }
                    results[idx] = std::move(pro);
                } else {
                    std::promise<std::string> pro = std::move(results[idx]);
                    results.erase(idx);
                    if (resp.success) {
                        pro.set_value(std::string(resp.content));
                    } else {
                        pro.set_exception(std::make_exception_ptr(
                            std::runtime_error(resp.content)));
                    }
                }
            }
        }).detach();
    }

    StatusOr<std::string> send(const std::string& cmd) {
        auto idx = reqQueue.post(cmd);
        std::future<std::string> fut;
        {
            std::unique_lock<std::mutex> l(mutex);
            if (results.find(idx) != results.end()) {
                fut = results[idx].get_future();
                results.erase(idx);
            } else {
                fut = results[idx].get_future();
            }
        }

        try {
            return fut.get();
        } catch (const std::exception& e) {
            return Status::Error("%s", e.what());
        }
    }

    void runCmd() {
        while (true) {
            BgRunReq req = reqQueue.wait();
            std::thread([this, req] {
                auto status = ProcessUtils::runCommand(req.content, false);
                BgRunResp resp;
                resp.idx = req.idx;
                if (!status.ok()) {
                    resp.success = false;
                    snprintf(resp.content, LineSize, "%s",
                             status.status().toString().c_str());
                } else {
                    resp.success = true;
                    snprintf(resp.content, LineSize, "%s",
                             status.value().c_str());
                }
                respQueue.post(resp);
            }).detach();
        }
    }
} *pBgRun = nullptr;

void ProcessUtils::bgRunCommand() {
    static std::once_flag once;
    std::call_once(once, [] () {
        static auto region = boost::interprocess::anonymous_shared_memory(sizeof(BgRun));
        pBgRun = new (region.get_address()) BgRun;

        static pid_t pid = ::fork();
        CHECK(pid >= 0);
        if (pid == 0) {
            ::prctl(PR_SET_PDEATHSIG, SIGKILL);
            ::prctl(PR_SET_NAME, "bg-run");
            pBgRun->runCmd();
            exit(0);
        }

        signal(SIGCHLD, [] (int) {
            while (true) {
                pid_t tPId = waitpid(-1, NULL, WNOHANG);
                LOG(INFO) << "SIGCHLD: " << tPId;
                if (tPId == pid) {
                    LOG(INFO) << "bg run exit.";
                    exit(0);
                }
            }
        });
    });
}

StatusOr<std::string> ProcessUtils::runCommand(const char* command, bool bg) {
    if (bg) {
        bgRunCommand();
        return pBgRun->send(command);
    }
    FILE* f = popen(command, "re");
    if (f == nullptr) {
        return Status::Error("Failed to execute the command [%s]", command);
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
        return Status::Error("Failed to read the output of the command. [%s]", command);
    }

    int st = pclose(f);
    if (!WIFEXITED(st)) {
        return Status::Error("Cmd Exist unexcepted. command: [%s]", command);
    }

    if (WEXITSTATUS(st)) {
        return folly::stringPrintf(
            "ERR: [%d], output: [%s], command: [%s]",
            WEXITSTATUS(st), out.str().c_str(), command);
    }
    return out.str();
}

}   // namespace nebula
