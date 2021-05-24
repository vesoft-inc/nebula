/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include <dirent.h>
#include <fnmatch.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/statfs.h>

namespace nebula {
namespace fs {

static const int32_t kMaxPathLen = 1024;

namespace detail {

bool removeDir(const char* path, bool recursively) {
    // Assuming the path is a directory
    DIR *dh = opendir(path);
    if (!dh) {
        LOG(ERROR) << "Failed to read the directory \"" << path
                   << "\" (" << errno << "): " << strerror(errno);
        return false;
    }

    bool succeeded = true;
    struct dirent *dEnt;
    errno = 0;
    while (succeeded && !!(dEnt = readdir(dh))) {
        if (!strcmp(dEnt->d_name, ".") || !strcmp(dEnt->d_name, "..")) {
            // Skip "." and ".."
            continue;
        }

        if (FileUtils::isDir(dEnt, path) && !recursively) {
            LOG(ERROR) << "Cannot remove the directory \"" << path
                       << "\" because it contains sub-directory \""
                       << dEnt->d_name << "\"";
            succeeded = false;
        } else {
            // Remove the directory entry, recursive call
            succeeded = FileUtils::remove(
                FileUtils::joinPath(path, dEnt->d_name).c_str(), recursively);
            if (!succeeded) {
                LOG(ERROR) << "Failed to remove \"" << dEnt->d_name
                           << "\" in \"" << path
                           << "\"";
            } else {
                VLOG(2) << "Succeeded removing \"" << dEnt->d_name << "\"";
            }
        }
    }

    if (succeeded && errno) {
        // There is an error
        LOG(ERROR) << "Failed to read the directory \"" << path
                   << "\" (" << errno << "): " << strerror(errno);
        succeeded = false;
    }

    if (closedir(dh)) {
        // Failed to close the directory stream
        LOG(ERROR) << "Failed to close the directory stream (" << errno
                   << "): " << strerror(errno);
        return false;
    }

    if (!succeeded) {
        LOG(ERROR) << "Failed to remove the content of the directory \""
                   << path << "\"";
        return false;
    }

    // All content has been removed, now remove the directory itself
    if (rmdir(path)) {
        LOG(ERROR) << "Failed to remove the directory \"" << path
                   << "\" (" << errno << "): " << strerror(errno);
        return false;
    }
    return true;
}

}  // namespace detail


StatusOr<std::string> FileUtils::readLink(const char *path) {
    char buffer[kMaxPathLen];
    auto len = ::readlink(path, buffer, kMaxPathLen);
    if (len == -1) {
        return Status::Error("readlink %s: %s", path, ::strerror(errno));
    }
    return std::string(buffer, len);
}

StatusOr<std::string> FileUtils::realPath(const char *path) {
    char *buffer  = ::realpath(path, NULL);
    if (buffer == NULL) {
        return Status::Error("realpath %s: %s", path, ::strerror(errno));
    }
    std::string truePath(buffer);
    ::free(buffer);
    return truePath;
}

std::string FileUtils::dirname(const char *path) {
    DCHECK(path != nullptr && *path != '\0');
    if (::strcmp("/", path) == 0) {     // root only
        return "/";
    }
    static const std::regex pattern("(.*)/([^/]+)/?");
    std::cmatch result;
    if (std::regex_match(path, result, pattern)) {
        if (result[1].first == result[1].second) {    // "/path" or "/path/"
            return "/";
        }
        return result[1].str();     // "/path/to", "path/to", or "path/to/"
    }
    return ".";
}


std::string FileUtils::basename(const char *path) {
    DCHECK(path != nullptr && *path != '\0');
    if (::strcmp("/", path) == 0) {
        return "";
    }
    static const std::regex pattern("(/*([^/]+/+)*)([^/]+)/?");
    std::cmatch result;
    std::regex_match(path, result, pattern);
    return result[3].str();
}


const char* FileUtils::getFileTypeName(FileType type) {
    static const char* kTypeNames[] = {
        "Unknown",
        "NotExist",
        "Regular",
        "Directory",
        "SoftLink",
        "CharDevice",
        "BlockDevice",
        "FIFO",
        "Socket"
    };

    return kTypeNames[static_cast<int>(type)];
}


size_t FileUtils::fileSize(const char* path) {
    struct stat st;
    if (lstat(path, &st)) {
        // Failed o get file stat
        VLOG(3) << "Failed to get information about \"" << path
                << "\" (" << errno << "): " << strerror(errno);
        return 0;
    }

    return st.st_size;
}


FileType FileUtils::fileType(const char* path) {
    struct stat st;
    if (lstat(path, &st)) {
        if (errno == ENOENT) {
            VLOG(3) << "The path \"" << path << "\" does not exist";
            return FileType::NOTEXIST;
        } else {
            // Failed o get file stat
            VLOG(3) << "Failed to get information about \"" << path
                    << "\" (" << errno << "): " << strerror(errno);
            return FileType::UNKNOWN;
        }
    }

    if (S_ISREG(st.st_mode)) {
        return FileType::REGULAR;
    } else if (S_ISDIR(st.st_mode)) {
        return FileType::DIRECTORY;
    } else if (S_ISLNK(st.st_mode)) {
        return FileType::SYM_LINK;
    } else if (S_ISCHR(st.st_mode)) {
        return FileType::CHAR_DEV;
    } else if (S_ISBLK(st.st_mode)) {
        return FileType::BLOCK_DEV;
    } else if (S_ISFIFO(st.st_mode)) {
        return FileType::FIFO;
    } else if (S_ISSOCK(st.st_mode)) {
        return FileType::SOCKET;
    }

    return FileType::UNKNOWN;
}


int64_t FileUtils::fileLastUpdateTime(const char* path) {
    struct stat st;
    if (lstat(path, &st)) {
        // Failed to get file stat
        LOG(ERROR) << "Failed to get file information for \"" << path
                   << "\" (" << errno << "): " << strerror(errno);
        return -1;
    }
    return st.st_mtime;
}


bool FileUtils::isStdinTTY() {
    return isFdTTY(::fileno(stdin));
}


bool FileUtils::isStdoutTTY() {
    return isFdTTY(::fileno(stdout));
}


bool FileUtils::isStderrTTY() {
    return isFdTTY(::fileno(stderr));
}


bool FileUtils::isFdTTY(int fd) {
    return ::isatty(fd) == 1;
}


std::string FileUtils::joinPath(const folly::StringPiece dir,
                                const folly::StringPiece filename) {
    std::string buf;

    std::size_t len = dir.size();
    if (len == 0) {
        buf.resize(filename.size() + 2);
        strcpy(&(buf[0]), "./");    // NOLINT
        strncpy(&(buf[2]), filename.begin(), filename.size());
        return buf;
    }

    if (dir[len-1] == '/') {
        buf.resize(len + filename.size());
        strncpy(&(buf[0]), dir.data(), len);
    } else {
        buf.resize(len + filename.size() + 1);
        strncpy(&(buf[0]), dir.data(), len);
        buf[len++] = '/';
    }

    strncpy(&(buf[len]), filename.data(), filename.size());

    return buf;
}


void FileUtils::dividePath(const folly::StringPiece path,
                           folly::StringPiece& parent,
                           folly::StringPiece& child) {
    if (path.empty() || path == "/") {
        // The given string is empty or just "/"
        parent = folly::StringPiece();
        child = path;
        return;
    }

    folly::StringPiece pathToLook =
        (path.back() == '/') ? folly::StringPiece(path.begin(), path.size() - 1)
                             : path;
    auto pos = pathToLook.rfind('/');
    if (pos == std::string::npos) {
        // Not found
        parent = folly::StringPiece();
        child = pathToLook;
        return;
    }

    // Found the last "/"
    child = folly::StringPiece(pathToLook.begin() + pos + 1,
                               pathToLook.size() - pos - 1);
    if (pos == 0) {
        // In the root directory
        parent = folly::StringPiece(pathToLook.begin(), 1);
    } else {
        parent = folly::StringPiece(pathToLook.begin(), pos);
    }
}


bool FileUtils::remove(const char* path, bool recursively) {
    auto type = fileType(path);
    switch (type) {
    case FileType::REGULAR:
    case FileType::SYM_LINK:
        // Regular file or link
        if (unlink(path)) {
            // Failed
            LOG(ERROR) << "Failed to remove the file \"" << path
                       << "\" (" << errno << "): " << strerror(errno);
            return false;
        }
        return true;
    case FileType::DIRECTORY:
        // Directory
        return detail::removeDir(path, recursively);
    case FileType::CHAR_DEV:
    case FileType::BLOCK_DEV:
    case FileType::FIFO:
    case FileType::SOCKET:
        LOG(ERROR) << "Only a directory, a regular file, or a soft link"
                   << " can be removed. But \"" << path << "\" is a "
                   << getFileTypeName(type);
        return false;
    case FileType::NOTEXIST:
        VLOG(2) << "The path \"" << path << "\" does not exist";
        return true;
    default:
        LOG(ERROR) << "We don't know the type of \"" << path << "\"";
        return false;
    }
}


bool FileUtils::makeDir(const std::string& dir, uint32_t mode) {
    if (dir.empty()) {
        return false;
    }
    FileType type = fileType(dir.c_str());
    if (type == FileType::DIRECTORY || type == FileType::SYM_LINK) {
        // The directory already exists
        return true;
    } else if (type != FileType::NOTEXIST) {
        // A file has existed, cannot create the directory
        return false;
    }

    folly::StringPiece parent;
    folly::StringPiece child;
    dividePath(dir, parent, child);

    // create parent if it is not empty
    if (!parent.empty()) {
        bool ret = makeDir(parent.toString(), mode);
        if (!ret) {
            return false;
        }
    }

    int err = mkdir(dir.c_str(), mode);
    if (err != 0) {
        return fileType(dir.c_str()) == FileType::DIRECTORY;
    }
    return true;
}

bool FileUtils::exist(const std::string& path) {
    if (path.empty()) {
        return false;
    }
    return access(path.c_str(), F_OK) == 0;
}

// static
bool FileUtils::rename(const std::string& src, const std::string& dst) {
    auto status = ::rename(src.c_str(), dst.c_str());
    LOG_IF(WARNING, status != 0) << "Rename " << src << " to " << dst << " failed, the errno: "
        << ::strerror(errno);
    return status == 0;
}

StatusOr<uint64_t> FileUtils::free(const char* path) {
    struct statfs diskInfo;
    int err = statfs(path, &diskInfo);
    if (err != 0) {
        return Status::Error("Failed to get diskInfo of %s", path);
    }
    return diskInfo.f_bfree * diskInfo.f_bsize;
}

StatusOr<uint64_t> FileUtils::available(const char* path) {
    struct statfs diskInfo;
    int err = statfs(path, &diskInfo);
    if (err != 0) {
        return Status::Error("Failed to get diskInfo of %s", path);
    }
    return diskInfo.f_bavail * diskInfo.f_bsize;
}

std::vector<std::string> FileUtils::listAllTypedEntitiesInDir(
        const char* dirpath,
        FileType type,
        bool returnFullPath,
        const char* namePattern) {
    std::vector<std::string> entities;
    struct dirent *dirInfo;
    DIR *dir = opendir(dirpath);
    if (dir == nullptr) {
        LOG(ERROR)<< "Failed to read the directory \"" << dirpath
                  << "\" (" << errno << "): " << strerror(errno);
        return entities;
    }

    while ((dirInfo = readdir(dir)) != nullptr) {
        if ((type == FileType::REGULAR && FileUtils::isReg(dirInfo, dirpath)) ||
            (type == FileType::DIRECTORY && FileUtils::isDir(dirInfo, dirpath)) ||
            (type == FileType::SYM_LINK && FileUtils::isLink(dirInfo, dirpath)) ||
            (type == FileType::CHAR_DEV && FileUtils::isChr(dirInfo, dirpath)) ||
            (type == FileType::BLOCK_DEV && FileUtils::isBlk(dirInfo, dirpath)) ||
            (type == FileType::FIFO && FileUtils::isFifo(dirInfo, dirpath)) ||
            (type == FileType::SOCKET && FileUtils::isSock(dirInfo, dirpath))) {
            if (!strcmp(dirInfo->d_name, ".") || !strcmp(dirInfo->d_name, "..")) {
                // Skip the "." and ".."
                continue;
            }
            if (namePattern &&
                fnmatch(namePattern, dirInfo->d_name, FNM_FILE_NAME | FNM_PERIOD)) {
                // Mismatched
                continue;
            }

            // We found one entity
            entities.emplace_back(
                returnFullPath ? joinPath(dirpath, std::string(dirInfo->d_name))
                               : std::string(dirInfo->d_name));
        }
    }
    closedir(dir);

    return entities;
}


std::vector<std::string> FileUtils::listAllFilesInDir(
        const char* dirpath,
        bool returnFullPath,
        const char* namePattern) {
    return listAllTypedEntitiesInDir(dirpath,
                                     FileType::REGULAR,
                                     returnFullPath,
                                     namePattern);
}


std::vector<std::string> FileUtils::listAllDirsInDir(
        const char* dirpath,
        bool returnFullPath,
        const char* namePattern) {
    return listAllTypedEntitiesInDir(dirpath,
                                     FileType::DIRECTORY,
                                     returnFullPath,
                                     namePattern);
}


FileUtils::Iterator::Iterator(std::string path, const std::regex *pattern)
    : path_(std::move(path)) {
    pattern_ = pattern;
    openFileOrDirectory();
    if (status_.ok()) {
        next();
    }
}


FileUtils::Iterator::~Iterator() {
    if (fstream_ != nullptr && fstream_->is_open()) {
        fstream_->close();
    }
    if (dir_ != nullptr) {
        ::closedir(dir_);
        dir_ = nullptr;
    }
}


void FileUtils::Iterator::next() {
    CHECK(valid());
    CHECK(type_ != FileType::UNKNOWN);
    while (true) {
        if (type_ == FileType::DIRECTORY) {
            dirNext();
        } else {
            fileNext();
        }
        if (!status_.ok()) {
            return;
        }
        if (pattern_ != nullptr) {
            if (!std::regex_search(entry_, matched_, *pattern_)) {
                continue;
            }
        }
        break;
    }
}


void FileUtils::Iterator::dirNext() {
    CHECK(type_ == FileType::DIRECTORY);
    CHECK(dir_ != nullptr);
    struct dirent *dent;
    while ((dent = ::readdir(dir_)) != nullptr) {
        if (dent->d_name[0] == '.') {
            continue;
        }
        break;
    }
    if (dent == nullptr) {
        status_ = Status::Error("EOF");
        return;
    }
    entry_ = dent->d_name;
}


void FileUtils::Iterator::fileNext() {
    CHECK(type_ == FileType::REGULAR);
    CHECK(fstream_ != nullptr);
    if (!std::getline(*fstream_, entry_)) {
        status_ = Status::Error("EOF");
    }
}


void FileUtils::Iterator::openFileOrDirectory() {
    type_ = FileUtils::fileType(path_.c_str());
    if (type_ == FileType::DIRECTORY) {
        if ((dir_ = ::opendir(path_.c_str())) == nullptr) {
            status_ = Status::Error("opendir `%s': %s", path_.c_str(), ::strerror(errno));
            return;
        }
    } else if (type_ == FileType::REGULAR) {
        fstream_ = std::make_unique<std::ifstream>();
        fstream_->open(path_);
        if (!fstream_->is_open()) {
            status_ = Status::Error("open `%s': %s", path_.c_str(), ::strerror(errno));
            return;
        }
    } else if (type_ == FileType::SYM_LINK) {
        auto result = FileUtils::realPath(path_.c_str());
        if (!result.ok()) {
            status_ = std::move(result).status();
            return;
        }
        path_ = std::move(result).value();
        openFileOrDirectory();
    } else {
        status_ = Status::Error("Filetype not supported `%s': %s",
                                path_.c_str(), FileUtils::getFileTypeName(type_));
        return;
    }
    status_ = Status::OK();
}


CHECK_TYPE(Reg, REGULAR, REG)
CHECK_TYPE(Dir, DIRECTORY, DIR)
CHECK_TYPE(Link, SYM_LINK, LNK)
CHECK_TYPE(Chr, CHAR_DEV, CHR)
CHECK_TYPE(Blk, BLOCK_DEV, BLK)
CHECK_TYPE(Fifo, FIFO, FIFO)
CHECK_TYPE(Sock, SOCKET, SOCK)
}  // namespace fs
}  // namespace nebula
