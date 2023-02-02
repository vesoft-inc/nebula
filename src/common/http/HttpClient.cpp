/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/http/HttpClient.h"

#include "curl/curl.h"
namespace nebula {

CurlHandle::CurlHandle() {
  curl_global_init(CURL_GLOBAL_ALL);
}

CurlHandle::~CurlHandle() {
  curl_global_cleanup();
}

CurlHandle* CurlHandle::instance() {
  static CurlHandle handle;
  return &handle;
}

HttpClient& HttpClient::instance() {
  static HttpClient instance_;
  return instance_;
}

HttpResponse HttpClient::get(const std::string& url) {
  return HttpClient::get(url, {});
}

HttpResponse HttpClient::get(const std::string& url, const std::vector<std::string>& header) {
  return sendRequest(url, "GET", header, "", "", "");
}

HttpResponse HttpClient::get(const std::string& url,
                             const std::vector<std::string>& header,
                             const std::string& username,
                             const std::string& password) {
  return sendRequest(url, "GET", header, "", username, password);
}

HttpResponse HttpClient::post(const std::string& url,
                              const std::vector<std::string>& header,
                              const std::string& body) {
  return sendRequest(url, "POST", header, body, "", "");
}

HttpResponse HttpClient::post(const std::string& url,
                              const std::vector<std::string>& header,
                              const std::string& body,
                              const std::string& username,
                              const std::string& password) {
  return sendRequest(url, "POST", header, body, username, password);
}

HttpResponse HttpClient::delete_(const std::string& url, const std::vector<std::string>& header) {
  return sendRequest(url, "DELETE", header, "", "", "");
}

HttpResponse HttpClient::delete_(const std::string& url,
                                 const std::vector<std::string>& header,
                                 const std::string& username,
                                 const std::string& password) {
  return sendRequest(url, "DELETE", header, "", username, password);
}

HttpResponse HttpClient::put(const std::string& url,
                             const std::vector<std::string>& header,
                             const std::string& body) {
  return sendRequest(url, "PUT", header, body, "", "");
}

HttpResponse HttpClient::put(const std::string& url,
                             const std::vector<std::string>& header,
                             const std::string& body,
                             const std::string& username,
                             const std::string& password) {
  return sendRequest(url, "PUT", header, body, username, password);
}

HttpResponse HttpClient::sendRequest(const std::string& url,
                                     const std::string& method,
                                     const std::vector<std::string>& header,
                                     const std::string& body,
                                     const std::string& username,
                                     const std::string& password) {
  CurlHandle::instance();
  HttpResponse resp;
  CURL* curl = curl_easy_init();
  CHECK(curl);
  setUrl(curl, url);
  if (folly::StringPiece(url).startsWith("https")) {
    setSSL(curl);
  }
  setMethod(curl, method);
  curl_slist* h = setHeaders(curl, header);
  if (!body.empty()) {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
  }
  setRespHeader(curl, resp.header);
  setRespBody(curl, resp.body);
  setTimeout(curl);
  if (!username.empty()) {
    setAuth(curl, username, password);
  }
  resp.curlCode = curl_easy_perform(curl);
  if (resp.curlCode != 0) {
    resp.curlMessage = std::string(curl_easy_strerror(resp.curlCode));
  }
  if (h) {
    curl_slist_free_all(h);
  }
  curl_easy_cleanup(curl);
  return resp;
}

void HttpClient::setUrl(CURL* curl, const std::string& url) {
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
}

void HttpClient::setMethod(CURL* curl, const std::string& method) {
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
}
curl_slist* HttpClient::setHeaders(CURL* curl, const std::vector<std::string>& headers) {
  curl_slist* h = nullptr;
  for (auto& header : headers) {
    h = curl_slist_append(h, header.c_str());
  }
  if (h) {
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, h);
  }
  return h;
}

void HttpClient::setRespHeader(CURL* curl, const std::string& header) {
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, onWriteData);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, &header);
}

void HttpClient::setRespBody(CURL* curl, const std::string& body) {
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, onWriteData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
}

void HttpClient::setTimeout(CURL* curl) {
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 1);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3);
}

void HttpClient::setAuth(CURL* curl, const std::string& username, const std::string& password) {
  if (!username.empty()) {
    curl_easy_setopt(curl, CURLOPT_USERNAME, username.c_str());
    if (!password.empty()) {
      curl_easy_setopt(curl, CURLOPT_PASSWORD, password.c_str());
    }
  }
}

void HttpClient::setSSL(CURL* curl) {
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
}

size_t HttpClient::onWriteData(void* ptr, size_t size, size_t nmemb, void* stream) {
  if (ptr == nullptr || size == 0) {
    return 0;
  }
  CHECK(stream);
  size_t realsize = size * nmemb;
  std::string* buffer = static_cast<std::string*>(stream);
  CHECK(buffer);
  buffer->append(static_cast<char*>(ptr), realsize);
  return realsize;
}
}  // namespace nebula
