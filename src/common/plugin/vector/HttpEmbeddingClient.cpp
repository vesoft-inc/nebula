/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/plugin/vector/HttpEmbeddingClient.h"
#include <folly/json.h>

namespace nebula {
namespace plugin {

HttpEmbeddingClient::HttpEmbeddingClient(nebula::HttpClient& httpClient,
                                       const std::string& url,
                                       const std::string& authToken)
    : httpClient_(httpClient)
    , url_(url)
    , authToken_(authToken) {}

HttpEmbeddingClient::~HttpEmbeddingClient() = default;

StatusOr<std::vector<float>> HttpEmbeddingClient::getEmbedding(
    const std::string& text,
    const std::string& modelName) {
    try {
        folly::dynamic request = folly::dynamic::object;
        request["text"] = text;
        request["model"] = modelName;

        std::vector<std::string> headers{"Content-Type: application/json"};
        if (!authToken_.empty()) {
            headers.emplace_back("Authorization: Bearer " + authToken_);
        }

        auto resp = httpClient_.post(url_ + "/embed", headers, folly::toJson(request));
        if (resp.curlCode != 0) {
            return Status::Error("HTTP request failed: %d, %s", resp.curlCode, resp.curlMessage.c_str());
        }

        auto json = folly::parseJson(resp.body);
        if (!json.isObject() || !json.count("embedding") || !json["embedding"].isArray()) {
            return Status::Error("Invalid response format");
        }

        std::vector<float> result;
        for (auto& v : json["embedding"]) {
            if (!v.isNumber()) {
                return Status::Error("Invalid embedding value type");
            }
            result.push_back(v.asDouble());
        }
        return result;
    } catch (const std::exception& e) {
        return Status::Error("Failed to get embedding: %s", e.what());
    }
}

StatusOr<std::vector<std::vector<float>>> HttpEmbeddingClient::batchGetEmbeddings(
    const std::vector<std::string>& texts,
    const std::string& modelName) {
    try {
        folly::dynamic request = folly::dynamic::object;
        request["texts"] = folly::dynamic::array(texts.begin(), texts.end());
        request["model"] = modelName;

        std::vector<std::string> headers{"Content-Type: application/json"};
        if (!authToken_.empty()) {
            headers.emplace_back("Authorization: Bearer " + authToken_);
        }

        auto resp = httpClient_.post(url_ + "/embed_batch", headers, folly::toJson(request));
        if (resp.curlCode != 0) {
            return Status::Error("HTTP request failed: %d, %s", resp.curlCode, resp.curlMessage.c_str());
        }

        auto json = folly::parseJson(resp.body);
        if (!json.isObject() || !json.count("embeddings") || !json["embeddings"].isArray()) {
            return Status::Error("Invalid response format");
        }

        std::vector<std::vector<float>> result;
        for (auto& embedding : json["embeddings"]) {
            if (!embedding.isArray()) {
                return Status::Error("Invalid embedding format");
            }
            std::vector<float> vec;
            for (auto& v : embedding) {
                if (!v.isNumber()) {
                    return Status::Error("Invalid embedding value type");
                }
                vec.push_back(v.asDouble());
            }
            result.push_back(std::move(vec));
        }
        return result;
    } catch (const std::exception& e) {
        return Status::Error("Failed to get batch embeddings: %s", e.what());
    }
}

}  // namespace plugin
}  // namespace nebula