/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_VECTOR_EMBEDDING_CLIENT_H_
#define COMMON_PLUGIN_VECTOR_EMBEDDING_CLIENT_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"

// Forward declarations
namespace nebula {
class HttpClient;
namespace plugin {

class EmbeddingClient {
public:
    virtual ~EmbeddingClient() = default;
    
    // Get embedding for single text
    virtual StatusOr<std::vector<float>> getEmbedding(
        const std::string& text,
        const std::string& modelName) = 0;
        
    // Get embeddings for multiple texts
    virtual StatusOr<std::vector<std::vector<float>>> batchGetEmbeddings(
        const std::vector<std::string>& texts,
        const std::string& modelName) = 0;
};

}  // namespace plugin
}  // namespace nebula

#endif  // COMMON_PLUGIN_VECTOR_EMBEDDING_CLIENT_H_ 