/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WEBSERVICE_GETSTATSHANDLER_H_
#define WEBSERVICE_GETSTATSHANDLER_H_

#include <folly/dynamic.h>                        // for dynamic
#include <proxygen/httpserver/RequestHandler.h>   // for RequestHandler
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError
#include <stdint.h>                               // for int64_t

#include <memory>  // for unique_ptr
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/base/Base.h"
#include "webservice/Common.h"  // for HttpCode, HttpCode:...

namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly
namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace nebula {

class GetStatsHandler : public proxygen::RequestHandler {
 public:
  GetStatsHandler() = default;

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 protected:
  virtual folly::dynamic getStats() const;
  void addOneStat(folly::dynamic& vals, const std::string& statName, int64_t statValue) const;
  void addOneStat(folly::dynamic& vals,
                  const std::string& statName,
                  const std::string& error) const;
  std::string toStr(folly::dynamic& vals) const;

 protected:
  HttpCode err_{HttpCode::SUCCEEDED};
  bool returnJson_{false};
  std::vector<std::string> statNames_;
};

}  // namespace nebula
#endif  // WEBSERVICE_GETFLAGSHANDLER_H_
