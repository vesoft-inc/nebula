/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <utility>
#include "tools/storage-client/src/NebulaStorageClient.h"
#include "tools/storage-client/src/lib/NebulaStorageClientImpl.h"

namespace nebula {
NebulaStorageClient::NebulaStorageClient(std::string  metaAddr)
    : metaAddr_(std::move(metaAddr)) {
}

NebulaStorageClient::~NebulaStorageClient() {
    delete client_;
}

ResultCode NebulaStorageClient::initArgs(int argc, char** argv) {
    return NebulaStorageClientImpl::initArgs(argc, argv);
}

ResultCode NebulaStorageClient::setLogging(const std::string& path) {
    return NebulaStorageClientImpl::setLogging(path);
}

ResultCode NebulaStorageClient::init(const std::string& spaceName, int32_t ioHandlers) {
    client_ = new NebulaStorageClientImpl(metaAddr_);
    return client_->init(spaceName, ioHandlers);
}

ResultCode NebulaStorageClient::lookup(const LookupContext& input, ResultSet& result) {
    return client_->lookup(input, result);
}

ResultCode NebulaStorageClient::getNeighbors(const NeighborsContext& input, ResultSet& result) {
    return client_->getNeighbors(input, result);
}

ResultCode NebulaStorageClient::getVertexProps(const VertexPropsContext& input, ResultSet& result) {
    return client_->getVertexProps(input, result);
}

}   // namespace nebula


