/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_BUFFERFLUSHER_H_
#define RAFTEX_BUFFERFLUSHER_H_

#include "base/Base.h"
#include "thread/NamedThread.h"
#include "raftex/InMemoryLogBuffer.h"

namespace vesoft {
namespace raftex {

class FileBasedWal;

class BufferFlusher final {
public:
    BufferFlusher();
    ~BufferFlusher();

    bool flushBuffer(std::shared_ptr<FileBasedWal> wal, BufferPtr buffer);

private:
    void flushLoop();

private:
    bool stopped_{false};

    std::queue<
        std::pair<std::shared_ptr<FileBasedWal>, BufferPtr>
    > buffers_;
    std::mutex buffersLock_;
    std::condition_variable bufferReadyCV_;

    thread::NamedThread flushThread_;
};

}  // namespace raftex
}  // namespace vesoft

#endif  // RAFTEX_BUFFERFLUSHER_H_

