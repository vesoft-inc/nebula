/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_VECTOR_EMBEDDING_CLIENT_H_
#define COMMON_PLUGIN_VECTOR_EMBEDDING_CLIENT_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/http/HttpClient.h"

// Add flag declarations
DECLARE_string(vector_embedding_url);
DECLARE_string(vector_embedding_auth_token);

namespace nebula {
namespace plugin {

class EmbeddingClient {
public:
    virtual ~EmbeddingClient() = default;
    
    // Get embedding for single text
    virtual StatusOr<std::vector<float>> getEmbedding(
        const std::string& text,
        const std::string& modelName) = 0;
        
    // Get embeddings for multiple texts in batch
    virtual StatusOr<std::vector<std::vector<float>>> batchGetEmbeddings(
        const std::vector<std::string>& texts,
        const std::string& modelName) = 0;
};

// HTTP-based embedding client implementation
class HttpEmbeddingClient : public EmbeddingClient {
public:
    explicit HttpEmbeddingClient(std::shared_ptr<HttpClient> client,
                                const std::string& url = FLAGS_vector_embedding_url,
                                const std::string& authToken = FLAGS_vector_embedding_auth_token)
        : client_(client)
        , url_(url)
        , authToken_(authToken) {}

    StatusOr<std::vector<float>> getEmbedding(
        const std::string& text,
        const std::string& modelName) override;
        
    StatusOr<std::vector<std::vector<float>>> batchGetEmbeddings(
        const std::vector<std::string>& texts,
        const std::string& modelName) override;

protected:
    // Helper method to prepare headers with auth token
    std::vector<std::string> prepareHeaders() const {
        std::vector<std::string> headers{"Content-Type: application/json"};
        if (!authToken_.empty()) {
            headers.emplace_back("Authorization: Bearer " + authToken_);
        }
        return headers;
    }

private:
    Status checkResponse(const HttpResponse& resp);
    
    std::shared_ptr<HttpClient> client_;
    std::string url_;
    std::string authToken_;
};

}  // namespace plugin
}  // namespace nebula

#endif  // COMMON_PLUGIN_VECTOR_EMBEDDING_CLIENT_H_ 