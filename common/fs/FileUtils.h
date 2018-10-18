/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_FS_FILEUTILS_H_
#define COMMON_FS_FILEUTILS_H_

#include "base/Base.h"

namespace vesoft {
namespace fs {

enum class FileType {
    UNKNOWN = 0,
    NOTEXIST,
    REGULAR,
    DIRECTORY,
    SYM_LINK,
    CHAR_DEV,
    BLOCK_DEV,
    FIFO,
    SOCKET
};


class FileUtils final {
public:
    FileUtils() = delete;

    // Get the running executable's path
    static std::string getExePath();
    // Get the running executable's current working direcotry
    static std::string getExeCWD();
    // Get the directory part of a path
    static std::string dirname(const char *path);
    // Get the base part of a path
    static std::string basename(const char *path);

    // return the size of the given file
    static size_t fileSize(const char* path);
    // return the type of the given file
    static FileType fileType(const char* path);
    // Return the file type name
    static const char* getFileTypeName(FileType type);
    // Return the last update time for the given file (UNIX Epoch time)
    static time_t fileLastUpdateTime(const char* path);

    // Tell if stdin attached to a TTY
    static bool isStdinTTY();
    // Tell if stdout atached to a TTY
    static bool isStdoutTTY();
    // Tell if stderr attached to a TTY
    static bool isStderrTTY();
    // Tell if the given fd attached to a TTY
    static bool isFdTTY(int fd);

    // Concatenate directory name and filename together
    //
    // If dir is empty, the result would be representation of the local file,
    // for example, joinPath("", "foo.bar") will return "./foo.bar"
    static std::string joinPath(const folly::StringPiece dir,
                                const folly::StringPiece filename);
    // Divide the given path into two parts: the parent directory path
    // (without the ending "/") and the filename or the child directory name
    //
    // For example, if the given path is
    //   "/a/b/c/d"
    // The result parent path would be "/a/b/c" and the child would be
    // "d"
    //
    // In the given path, the ending "/" will be ignored. For example, when
    // the given path is "/a/b/c/d/", the result parent path would be "/a/b/c",
    // and the child name would be "d"
    //
    // When the given path is in the root directory, such as "/a", the
    // result parent would be "/" (this is the only case that the parent
    // will end with "/"), and the child is "a"
    //
    // When the given path is the root directory itself, the result parent
    // would be empty, and the child is "/"
    //
    // Parameters: path -- the given path
    //             parent -- return the parent directory path. It ends
    //                       with the directory name without "/", unless
    //                       the parent directory is the root directory
    //             child -- return the filename or the child directory
    //                      name
    static void dividePath(const folly::StringPiece path,
                           folly::StringPiece& parent,
                           folly::StringPiece& child);

    // Remove the given file or directory (with its content)
    //
    // When the given path is a directory and recursively == false,
    // removal will fail if the directory is not empty. If recursive == true,
    // all content will be removed, including the sub-directories and their
    // contents
    //
    // The method returns true if removal succeeded or the given file
    // or directory does not exist.
    static bool remove(const char* path, bool recursively = false);
    // Create the directory
    //
    // It will make the parent directories as needed
    // (much like commandline "mkdir -p")
    static bool makeDir(const std::string& dir);

    /**
     * List all entities in the given directory, whose type matches
     * the given file type
     *
     * The function returns either fullpath or just the filename
     *
     * You can also pass in a wildcard pattern, which makes the function
     * to only return entities whose names match the pattern
     */
    static std::vector<std::string> listAllTypedEntitiesInDir(
        const char* dirpath,
        FileType type,
        bool returnFullPath,
        const char* namePattern);
    /**
     * List all files in the given directory
     *
     * Internally, it calls listAllTypedEntitiesInDir()
     */
    static std::vector<std::string> listAllFilesInDir(
        const char* dirpath,
        bool returnFullPath = false,
        const char* namePattern = nullptr);
    /**
     * List all sub-directories in the given directory
     *
     * Internally, it calls listAllTypedEntitiesInDir()
     */
    static std::vector<std::string> listAllDirsInDir(
        const char* dirpath,
        bool returnFullPath = false,
        const char* namePattern = nullptr);
};

}  // namespace fs
}  // namespace vesoft

#endif  // COMMON_FS_FILEUTILS_H_

