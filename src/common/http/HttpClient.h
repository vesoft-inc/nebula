/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_HTTP_HTTPCLIENT_H_
#define COMMON_HTTP_HTTPCLIENT_H_
#include <memory>
#include <mutex>
#include <string>

#include "common/base/StatusOr.h"
#include "curl/curl.h"

namespace nebula {

class CurlHandle {
 public:
  static CurlHandle* instance();

 private:
  CurlHandle();
  ~CurlHandle();
};

struct HttpResponse {
  CURLcode curlCode;
  std::string curlMessage;
  std::string header;
  std::string body;
};

class HttpClient {
 public:
  static HttpClient& instance();

  virtual ~HttpClient() = default;

  virtual HttpResponse get(const std::string& url);

  virtual HttpResponse get(const std::string& url, const std::vector<std::string>& headers);

  virtual HttpResponse get(const std::string& url,
                           const std::vector<std::string>& headers,
                           const std::string& username,
                           const std::string& password);

  virtual HttpResponse post(const std::string& url,
                            const std::vector<std::string>& headers,
                            const std::string& body);

  virtual HttpResponse post(const std::string& url,
                            const std::vector<std::string>& headers,
                            const std::string& body,
                            const std::string& username,
                            const std::string& password);

  virtual HttpResponse delete_(const std::string& url, const std::vector<std::string>& headers);

  virtual HttpResponse delete_(const std::string& url,
                               const std::vector<std::string>& headers,
                               const std::string& username,
                               const std::string& password);

  virtual HttpResponse put(const std::string& url,
                           const std::vector<std::string>& headers,
                           const std::string& body);

  virtual HttpResponse put(const std::string& url,
                           const std::vector<std::string>& headers,
                           const std::string& body,
                           const std::string& username,
                           const std::string& password);

 protected:
  HttpClient() = default;

 private:
  static HttpResponse sendRequest(const std::string& url,
                                  const std::string& method,
                                  const std::vector<std::string>& headers,
                                  const std::string& body,
                                  const std::string& username,
                                  const std::string& password);
  static void setUrl(CURL* curl, const std::string& url);
  static void setMethod(CURL* curl, const std::string& method);
  static curl_slist* setHeaders(CURL* curl, const std::vector<std::string>& headers);

  static void setRespBody(CURL* curl, const std::string& body);
  static void setRespHeader(CURL* curl, const std::string& header);
  static void setTimeout(CURL* curl);
  static void setSSL(CURL* curl);
  static void setAuth(CURL* curl, const std::string& username, const std::string& password);
  static size_t onWriteData(void* ptr, size_t size, size_t nmemb, void* stream);
};

}  // namespace nebula

#endif
