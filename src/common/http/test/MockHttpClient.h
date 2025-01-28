/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_HTTP_TEST_MOCKHTTPCLIENT_H_
#define COMMON_HTTP_TEST_MOCKHTTPCLIENT_H_

#include "common/http/HttpClient.h"

namespace nebula {

class MockHttpClient : public HttpClient {
public:
    void setResponse(HttpResponse& resp) {
        response_ = resp;
    }

    // Add getter for last request headers
    const std::vector<std::string>& getLastHeaders() const {
        return lastHeaders_;
    }

    HttpResponse get([[maybe_unused]] const std::string& url) override {
        return response_;
    }

    HttpResponse get([[maybe_unused]] const std::string& url, 
                    [[maybe_unused]] const std::vector<std::string>& headers) override {
        return response_;
    }

    HttpResponse get([[maybe_unused]] const std::string& url,
                    [[maybe_unused]] const std::vector<std::string>& headers,
                    [[maybe_unused]] const std::string& username,
                    [[maybe_unused]] const std::string& password) override {
        return response_;
    }

    HttpResponse post([[maybe_unused]] const std::string& url,
                     const std::vector<std::string>& headers,
                     [[maybe_unused]] const std::string& body) override {
        // Store headers for verification
        lastHeaders_ = headers;
        return response_;
    }

    HttpResponse post([[maybe_unused]] const std::string& url,
                     [[maybe_unused]] const std::vector<std::string>& headers,
                     [[maybe_unused]] const std::string& body,
                     [[maybe_unused]] const std::string& username,
                     [[maybe_unused]] const std::string& password) override {
        return response_;
    }

    HttpResponse delete_([[maybe_unused]] const std::string& url,
                        [[maybe_unused]] const std::vector<std::string>& headers) override {
        return response_;
    }

    HttpResponse delete_([[maybe_unused]] const std::string& url,
                        [[maybe_unused]] const std::vector<std::string>& headers,
                        [[maybe_unused]] const std::string& username,
                        [[maybe_unused]] const std::string& password) override {
        return response_;
    }

    HttpResponse put([[maybe_unused]] const std::string& url,
                    [[maybe_unused]] const std::vector<std::string>& headers,
                    [[maybe_unused]] const std::string& body) override {
        return response_;
    }

    HttpResponse put([[maybe_unused]] const std::string& url,
                    [[maybe_unused]] const std::vector<std::string>& headers,
                    [[maybe_unused]] const std::string& body,
                    [[maybe_unused]] const std::string& username,
                    [[maybe_unused]] const std::string& password) override {
        return response_;
    }

private:
    HttpResponse response_;
    std::vector<std::string> lastHeaders_;  // Store the last request headers
};

}  // namespace nebula

#endif  // COMMON_HTTP_TEST_MOCKHTTPCLIENT_H_ 