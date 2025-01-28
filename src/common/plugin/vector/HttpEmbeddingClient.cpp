/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/plugin/vector/EmbeddingClient.h"
#include <folly/json.h>

DECLARE_string(vector_embedding_url);
DECLARE_string(vector_embedding_auth_token);
DECLARE_int32(vector_embedding_timeout_ms);
DECLARE_int32(vector_embedding_retry_times);
DECLARE_int32(vector_embedding_batch_size);

namespace nebula {
namespace plugin {

StatusOr<std::vector<float>> HttpEmbeddingClient::getEmbedding(
    const std::string& text,
    const std::string& modelName) {
    
    try {
        folly::dynamic request = folly::dynamic::object;
        request["text"] = text;
        request["model"] = modelName;

        auto resp = client_->post(url_ + "/embed",
                                prepareHeaders(),
                                folly::toJson(request));
                                
        NG_RETURN_IF_ERROR(checkResponse(resp));
        
        auto json = folly::parseJson(resp.body);
        auto& embedding = json["embedding"];
        std::vector<float> result;
        result.reserve(embedding.size());
        
        for (const auto& v : embedding) {
            result.push_back(v.asDouble());
        }
        return result;
    } catch (const std::exception& e) {
        return Status::Error("Failed to parse embedding response: %s", e.what());
    }
}

StatusOr<std::vector<std::vector<float>>> HttpEmbeddingClient::batchGetEmbeddings(
    const std::vector<std::string>& texts,
    const std::string& modelName) {
    
    try {
        folly::dynamic request = folly::dynamic::object;
        request["texts"] = folly::dynamic::array(texts.begin(), texts.end());
        request["model"] = modelName;
        
        auto resp = client_->post(url_ + "/embed_batch", 
                                prepareHeaders(),
                                folly::toJson(request));
        
        NG_RETURN_IF_ERROR(checkResponse(resp));
        
        auto json = folly::parseJson(resp.body);
        auto& embeddings = json["embeddings"];
        std::vector<std::vector<float>> result;
        result.reserve(embeddings.size());
        
        for (const auto& embedding : embeddings) {
            std::vector<float> vec;
            vec.reserve(embedding.size());
            for (const auto& v : embedding) {
                vec.push_back(v.asDouble());
            }
            result.push_back(std::move(vec));
        }
        return result;
    } catch (const std::exception& e) {
        return Status::Error("Failed to parse batch embedding response: %s", e.what());
    }
}

Status HttpEmbeddingClient::checkResponse(const HttpResponse& resp) {
    if (resp.curlCode != 0) {
        return Status::Error("HTTP request failed: %d, %s", resp.curlCode, resp.curlMessage.c_str());
    }
    return Status::OK();
}

}  // namespace plugin
}  // namespace nebula 