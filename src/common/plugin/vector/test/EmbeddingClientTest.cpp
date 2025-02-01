/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include "common/plugin/vector/HttpEmbeddingClient.h"
#include "common/http/test/MockHttpClient.h"

namespace nebula {
namespace plugin {

class EmbeddingClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        mockClient_ = std::make_unique<MockHttpClient>();
        client_ = std::make_unique<HttpEmbeddingClient>(
            *mockClient_,  // Pass by reference
            "http://localhost:8000",  // Test URL
            "test-token");            // Test auth token
    }

    void verifyAuthHeader(const MockHttpClient* client) {
        const auto& headers = client->getLastHeaders();
        bool hasAuth = false;
        bool hasContentType = false;
        
        for (const auto& header : headers) {
            if (header == "Authorization: Bearer test-token") {
                hasAuth = true;
            }
            if (header == "Content-Type: application/json") {
                hasContentType = true;
            }
        }
        
        ASSERT_TRUE(hasAuth) << "Auth header not found";
        ASSERT_TRUE(hasContentType) << "Content-Type header not found";
    }

    std::unique_ptr<MockHttpClient> mockClient_;  // Own the mock client
    std::unique_ptr<HttpEmbeddingClient> client_;
};

TEST_F(EmbeddingClientTest, GetEmbedding) {
    HttpResponse resp;
    resp.curlCode = CURLE_OK;
    resp.body = R"({"embedding": [0.1, 0.2, 0.3]})";
    mockClient_->setResponse(resp);

    auto result = client_->getEmbedding("test text", "test-model");
    ASSERT_TRUE(result.ok()) << result.status();
    
    const auto& embedding = result.value();
    ASSERT_EQ(embedding.size(), 3);
    EXPECT_FLOAT_EQ(embedding[0], 0.1);
    EXPECT_FLOAT_EQ(embedding[1], 0.2);
    EXPECT_FLOAT_EQ(embedding[2], 0.3);
    
    verifyAuthHeader(mockClient_.get());
}

TEST_F(EmbeddingClientTest, GetEmbeddingError) {
    HttpResponse resp;
    resp.curlCode = CURLE_COULDNT_CONNECT;
    resp.curlMessage = "Failed to connect";
    mockClient_->setResponse(resp);

    auto result = client_->getEmbedding("test text", "test-model");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "HTTP request failed: 7, Failed to connect");
}

TEST_F(EmbeddingClientTest, GetEmbeddingInvalidJson) {
    HttpResponse resp;
    resp.curlCode = CURLE_OK;
    resp.body = "invalid json";
    mockClient_->setResponse(resp);

    auto result = client_->getEmbedding("test text", "test-model");
    ASSERT_FALSE(result.ok());
    EXPECT_NE(result.status().toString().find("Failed to parse"), 
              std::string::npos) << "Error message: " << result.status();
}

TEST_F(EmbeddingClientTest, BatchGetEmbeddings) {
    HttpResponse resp;
    resp.curlCode = CURLE_OK;
    resp.body = R"({
        "embeddings": [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
        ]
    })";
    mockClient_->setResponse(resp);

    std::vector<std::string> texts = {"text1", "text2"};
    auto result = client_->batchGetEmbeddings(texts, "test-model");
    ASSERT_TRUE(result.ok()) << result.status();
    
    const auto& embeddings = result.value();
    ASSERT_EQ(embeddings.size(), 2);
    ASSERT_EQ(embeddings[0].size(), 3);
    ASSERT_EQ(embeddings[1].size(), 3);
    
    EXPECT_FLOAT_EQ(embeddings[0][0], 0.1);
    EXPECT_FLOAT_EQ(embeddings[1][1], 0.5);
    
    verifyAuthHeader(mockClient_.get());
}

TEST_F(EmbeddingClientTest, BatchGetEmbeddingsError) {
    HttpResponse resp;
    resp.curlCode = CURLE_COULDNT_CONNECT;
    resp.curlMessage = "Failed to connect";
    mockClient_->setResponse(resp);

    std::vector<std::string> texts = {"text1", "text2"};
    auto result = client_->batchGetEmbeddings(texts, "test-model");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "HTTP request failed: 7, Failed to connect");
}

}  // namespace plugin
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
} 