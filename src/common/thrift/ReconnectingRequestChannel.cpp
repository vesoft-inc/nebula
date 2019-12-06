/*
 * Copyright 2018-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <thrift/lib/cpp2/async/ReconnectingRequestChannel.h>

#include <thrift/lib/cpp2/async/ClientChannel.h>

#include <folly/io/async/AsyncSocketException.h>

namespace apache {
namespace thrift {

class ReconnectingRequestChannel::RequestCallback
    : public apache::thrift::RequestCallback {
 public:
  RequestCallback(
      ReconnectingRequestChannel& channel,
      std::unique_ptr<apache::thrift::RequestCallback> cob)
      : channel_(channel), impl_(channel_.impl_), cob_(std::move(cob)) {}

  void requestSent() override {
    cob_->requestSent();
  }

  void replyReceived(apache::thrift::ClientReceiveState&& state) override {
    handleTransportException(state);
    cob_->replyReceived(std::move(state));
  }

  void requestError(apache::thrift::ClientReceiveState&& state) override {
    handleTransportException(state);
    cob_->requestError(std::move(state));
  }

 private:
  void handleTransportException(apache::thrift::ClientReceiveState& state) {
    if (!state.isException()) {
      return;
    }
    if (!state.exception()
             .is_compatible_with<
                 apache::thrift::transport::TTransportException>()) {
      return;
    }
    if (channel_.impl_ != impl_) {
      return;
    }
    channel_.impl_.reset();
  }

  ReconnectingRequestChannel& channel_;
  ReconnectingRequestChannel::ImplPtr impl_;
  std::unique_ptr<apache::thrift::RequestCallback> cob_;
};

uint32_t ReconnectingRequestChannel::sendRequest(
    apache::thrift::RpcOptions& options,
    std::unique_ptr<apache::thrift::RequestCallback> cob,
    std::unique_ptr<apache::thrift::ContextStack> ctx,
    std::unique_ptr<folly::IOBuf> buf,
    std::shared_ptr<apache::thrift::transport::THeader> header) {
  cob = std::make_unique<RequestCallback>(*this, std::move(cob));

  return impl().sendRequest(
      options,
      std::move(cob),
      std::move(ctx),
      std::move(buf),
      std::move(header));
}

ReconnectingRequestChannel::Impl& ReconnectingRequestChannel::impl() {
  if (!impl_ || !std::dynamic_pointer_cast<apache::thrift::ClientChannel>(impl_)->good()) {
    impl_ = implCreator_(evb_);
  }

  return *impl_;
}
}  // namespace thrift
}  // namespace apache
