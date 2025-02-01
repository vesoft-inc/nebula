/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_VECTOR_HTTP_EMBEDDING_CLIENT_H_
#define COMMON_PLUGIN_VECTOR_HTTP_EMBEDDING_CLIENT_H_

#include "common/base/Base.h"
#include "common/http/HttpClient.h"
#include "common/plugin/vector/EmbeddingClient.h"

// Flag declarations
DECLARE_string(vector_embedding_url);
DECLARE_string(vector_embedding_auth_token);

namespace nebula {
namespace plugin {

class HttpEmbeddingClient final : public EmbeddingClient {
public:
    explicit HttpEmbeddingClient(nebula::HttpClient& httpClient,
                                const std::string& url,
                                const std::string& authToken);

    ~HttpEmbeddingClient() override;

    StatusOr<std::vector<float>> getEmbedding(
        const std::string& text,
        const std::string& modelName) override;

    StatusOr<std::vector<std::vector<float>>> batchGetEmbeddings(
        const std::vector<std::string>& texts,
        const std::string& modelName) override;

private:
    Status checkResponse(const HttpResponse& resp);
    std::vector<std::string> prepareHeaders() const;

    nebula::HttpClient& httpClient_;
    std::string url_;
    std::string authToken_;
};

}  // namespace plugin
}  // namespace nebula

#endif  // COMMON_PLUGIN_VECTOR_HTTP_EMBEDDING_CLIENT_H_ 