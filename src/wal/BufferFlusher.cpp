/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "wal/BufferFlusher.h"
#include "wal/FileBasedWal.h"

namespace nebula {
namespace wal {

BufferFlusher::BufferFlusher()
    : flushThread_("Buffer flusher",
                   std::bind(&BufferFlusher::flushLoop, this)) {
}


BufferFlusher::~BufferFlusher() {
    {
        std::lock_guard<std::mutex> g(buffersLock_);
        stopped_ = true;
    }

    bufferReadyCV_.notify_one();

    flushThread_.join();
    CHECK(buffers_.empty());
}


bool BufferFlusher::flushBuffer(std::shared_ptr<FileBasedWal> wal,
                                BufferPtr buffer) {
    {
        std::lock_guard<std::mutex> g(buffersLock_);

        if (stopped_) {
            LOG(ERROR) << "Buffer flusher has stopped";
            return false;
        }

        buffers_.emplace(std::move(wal), std::move(buffer));
    }

    // Notify the loop thread
    bufferReadyCV_.notify_one();

    return true;
}


void BufferFlusher::flushLoop() {
    LOG(INFO) << "Buffer flusher loop started";

    while(true) {
        decltype(buffers_)::value_type bufferPair;
        {
            std::unique_lock<std::mutex> g(buffersLock_);
            if (buffers_.empty()) {
                if (stopped_) {
                    VLOG(1) << "The buffer flusher has stopped,"
                               " so exiting the flush loop";
                    break;
                }
                // Otherwise need to wait
                bufferReadyCV_.wait(g, [this] {
                    return !buffers_.empty() || stopped_;
                });
            } else {
                bufferPair = std::move(buffers_.front());
                buffers_.pop();

                bufferPair.first->flushBuffer(bufferPair.second);
            }
        }
    }

    LOG(INFO) << "Buffer flusher loop finished";
}

}  // namespace wal
}  // namespace nebula

