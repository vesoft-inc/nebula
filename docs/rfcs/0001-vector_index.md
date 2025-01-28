---
name: Vector Index Support
about: Basic vector similarity search using Elasticsearch KNN

---

# Summary

Add basic vector similarity search support to NebulaGraph by leveraging Elasticsearch's Brute Force and/or KNN capabilities and existing ES integration infrastructure.

# Motivation

Enable simple vector similarity search on string properties by:
1. Reusing existing Elasticsearch integration
2. Using remote embedding service to convert text to vectors
3. Starting with basic KNN search functionality

# Usage explanation

## Vector Index Creation

```ngql
-- Create vector index on tag/edge property
CREATE VECTOR INDEX <index_name> ON <tag/edge_name>(<prop_name>) 
WITH DIMENSION = <dimension>, MODEL = "<model_profile_name>";
```

> Note: we'll have model endpoint, credential, name, etc in config file, and we'll use model name to find the endpoint and credential.

## Vector Search 

```ngql
-- Basic vector similarity search
LOOKUP ON <tag/edge_name>
WHERE VECTOR_QUERY(<index_name>, "text to convert to vector") 
YIELD id(vertex), score() as similarity;
```

# Design explanation

## Implementation Status

### Phase 1 (Completed)

1. Meta Service:
   - [x] Add vector index metadata to MetaKeyUtils
     - [x] Add to tableMaps
     - [x] Define kVectorIndexTable constant
     - [x] Implement vector index key/value functions
   - [x] MetaServiceHandler handlers for vector index operations
   - [x] VectorIndexProcessor implementation following FTIndex pattern
   - [x] Proper error handling and validation
   - [x] Consistent locking mechanism

2. Thrift Interface:
   - [x] Add VectorIndex struct
   - [x] Add CreateVectorIndexReq
   - [x] Add DropVectorIndexReq
   - [x] Add ListVectorIndexesReq/Resp

3. Meta Client:
   - [x] CRUD methods for vector index operations
   - [x] Cache-related methods aligned with FTIndex
   - [x] vectorIndexMap_ in MetaData struct
   - [x] Error handling and response processing

4. Testing:
   - [x] Basic unit test setup
   - [x] Mock ES adapter for testing
   - [x] Test successful index creation
   - [x] Test invalid field type handling
   - [x] Test list and drop operations
   - [ ] Test duplicate index creation
   - [ ] Test edge cases (invalid space, missing field, etc)
   - [ ] Integration tests

### Phase 2

1. Expression Support:
   - [x] Add VectorQueryExpression type
   - [x] Add VectorQueryArgument class
   - [x] Add parser support for VECTOR_QUERY


2. Parser Integration:
   - [x] Add VECTOR_QUERY token
   - [x] Add vector_query_argument grammar
   - [x] Add vector_query_expression grammar
   - [x] Add lookup_where_clause support
   - [x] Add test cases

### Phase 3 (In Progress)

1. Configuration & Embedding Client (Completed)
   - [x] Add vector embedding gflags to listener configuration
   - [x] Create EmbeddingClient interface
   - [x] Implement basic HTTP embedding client with token-based auth
   - [x] Add comprehensive unit tests for embedding client
   - [x] Add proper error handling and response validation
   - [x] Add batch processing support

2. Index Creation Enhancement (Next)
   - [ ] Add dense_vector mapping support in ESAdapter
   - [ ] Update createIndex flow for vector indexes
   - [ ] Add vector-specific ES configurations
   - [ ] Add tests for vector mapping creation

3. ESAdapter Vector Operations
   - [ ] Add script_score query support
   - [ ] Add vector search methods
   - [ ] Add bulk operation support for vectors
   - [ ] Add tests for vector operations

4. Listener Enhancement
   - [ ] Update pickTagAndEdgeData for vector indexes
   - [ ] Add vector conversion in data pipeline
   - [ ] Add error handling for embedding operations
   - [ ] Add tests for vector data writing

5. Query Implementation
   - [ ] Implement vector similarity search
   - [ ] Add score normalization
   - [ ] Add query optimization
   - [ ] Add integration tests

### Implementation Notes

1. Embedding Client Implementation:

```cpp
// Abstract embedding client interface
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

// HTTP-based implementation with token auth
class HttpEmbeddingClient : public EmbeddingClient {
public:
    explicit HttpEmbeddingClient(std::shared_ptr<HttpClient> client,
                                const std::string& url = FLAGS_vector_embedding_url,
                                const std::string& authToken = FLAGS_vector_embedding_auth_token)
        : client_(client)
        , url_(url)
        , authToken_(authToken) {}

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
    std::shared_ptr<HttpClient> client_;
    std::string url_;
    std::string authToken_;
};
```



2. ESListener Enhancement:

```cpp
// Key changes to ESListener
class ESListener {
private:
    // Add vector index support to existing pickTagAndEdgeData
    void pickTagAndEdgeData(BatchLogType type,
                           const std::string& key,
                           const std::string& value,
                           const PickFunc& callback) {
        // Existing fulltext index handling...
        auto ftIndexes = getFTIndexes(...);
        
        // Add vector index handling
        auto vectorIndexes = getVectorIndexes(...);
        for (auto& index : vectorIndexes) {
            if (type == BatchLogType::OP_BATCH_PUT) {
                // Get text value from field
                auto text = reader->getValueByName(index.second.get_field());
                if (text.type() != Value::Type::STRING) {
                    continue;
                }
                
                // Get embedding from model service
                auto embedding = getEmbedding(text.getStr(), 
                                           index.second.get_model_name(),
                                           index.second.get_dimension());
                
                // Add to ES bulk request
                data["vector_field"] = embedding;
                data["text_field"] = text.getStr();  // Store original text too
            }
            callback(type, indexName, vid, src, dst, rank, std::move(data));
        }
    }

    // Add embedding client support
    StatusOr<std::vector<float>> getEmbedding(
        const std::string& text,
        const std::string& modelName,
        int32_t dimension) {
        // Use configured embedding service
        return embeddingClient_->getEmbedding(text, modelName);
    }

private:
    // Add new member for embedding client
    std::unique_ptr<EmbeddingClient> embeddingClient_;
};
```

3. ESAdapter Enhancement:

```cpp
class ESAdapter {
public:
    // Add vector mapping support
    Status createIndex(const std::string& index, 
                      const meta::cpp2::VectorIndex* vectorIndex = nullptr) {
        if (vectorIndex != nullptr) {
            // Create mapping with dense_vector
            return createVectorMapping(index, 
                                    vectorIndex->get_dimension(),
                                    vectorIndex->get_similarity_metric());
        }
        // Existing fulltext mapping creation
        return createFulltextMapping(index);
    }

    // Add vector search support
    Status vectorSearch(const std::string& index,
                       const std::string& field,
                       const std::vector<float>& vector,
                       int32_t topK = 10) {
        // Use script_score query
        return scriptScoreQuery(index, field, vector, topK);
    }
};
```

### Configuration

The following configuration options are now implemented:
- `--vector_embedding_url`: URL of the embedding service endpoint
- `--vector_embedding_auth_token`: Bearer token for authentication
- `--vector_embedding_batch_size`: Maximum batch size for embedding requests
- `--vector_embedding_timeout_ms`: Timeout for embedding requests
- `--vector_embedding_retry_times`: Number of retries for failed requests

### Testing Status
- [x] Basic embedding client tests
- [x] Error handling tests
- [x] Authentication header verification
- [x] Batch processing tests
- [x] Mock HTTP client implementation
- [x] JSON response parsing tests


### Phase 3 (Planned)

1. Vector Type Support:
   - [ ] Add native VECTOR type
   - [ ] Update schema validation for vector fields
   - [ ] Add dimension validation

2. Advanced Features:
   - [ ] Add more similarity metrics
   - [ ] Support configurable index parameters
   - [ ] Add bulk operations
   - [ ] Add index status tracking
   - [ ] Add HNSW index support (requires ES version bump)

3. Performance:
   - [ ] Add benchmarking tests
   - [ ] Optimize similarity search
   - [ ] Add caching strategies
   - [ ] Performance testing and tuning

# Compatibility

The implementation follows existing index patterns (particularly FTIndex) to maintain consistency and compatibility with existing code.

# Drawbacks

1. Limited to basic KNN initially
2. Depends on remote embedding service
3. Basic error handling only

# Prior art

1. Elasticsearch dense_vector type and KNN search
2. Existing NebulaGraph fulltext index implementation

# Unresolved questions

1. Error handling for embedding service
2. Optimal vector dimension size
3. Performance characteristics

# Future possibilities

1. Add HNSW index support(need to bump Elasticsearch version)
2. Improved error handling
3. Performance optimization
4. Additional similarity metrics

# Vector Index Support

## Background

To support vector similarity search in Nebula Graph, we need to add vector index functionality. This will allow users to create indexes on vector fields and perform similarity searches efficiently.

## Embedding Service Requirements

### API Compatibility
- The embedding service must provide an OpenAI-compatible API endpoint
- The endpoint should accept text input and return vector embeddings
- The service should support batch processing for efficiency

### API Format
Request format:
```json
{
  "input": "text to embed",
  "model": "text-embedding-ada-002",  // Or compatible model
  "encoding_format": "float"
}
```

Response format:
```json
{
  "data": [{
    "embedding": [0.1, -0.05, ...],  // Vector of floats
    "index": 0
  }],
  "model": "text-embedding-ada-002",
  "usage": {
    "prompt_tokens": 5,
    "total_tokens": 5
  }
}
```

### Configuration
The following configuration options will be required:
- `--vector_embedding_url`: URL of the embedding service endpoint
- `--vector_embedding_key`: API key for authentication (optional)
- `--vector_embedding_batch_size`: Maximum batch size for embedding requests
- `--vector_embedding_timeout_ms`: Timeout for embedding requests
- `--vector_embedding_retry_times`: Number of retries for failed requests

### Error Handling
The embedding client should handle:
1. Connection errors (retry with backoff)
2. Authentication errors
3. Rate limiting
4. Invalid input errors
5. Timeout errors

### Implementation Plan
1. Create an EmbeddingClient class similar to ESClient
2. Implement vector conversion in VectorIndexUtils using the client
3. Add configuration options to nebula-graphd
4. Add error handling and retry logic
5. Add metrics for embedding service calls

## Design Details

### Interface Design (Completed)

We have implemented the following interfaces in meta.thrift:

```thrift
struct VectorIndex {
    1: common.GraphSpaceID  space_id,
    2: common.SchemaID      depend_schema,
    3: string              field,
    4: i32                 dimension,
    5: string              model_endpoint,
    6: optional string     similarity_metric = "cosine"  // cosine, dot_product, euclidean
}

struct CreateVectorIndexReq {
    1: binary              vector_index_name,
    2: VectorIndex         index,
}

struct DropVectorIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              vector_index_name,
}

struct ListVectorIndexesReq {
}

struct ListVectorIndexesResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: map<binary, VectorIndex> (cpp.template = "std::unordered_map") indexes,
}
```

### Implementation Status

#### Meta Service (Completed)
- [x] MetaServiceHandler handlers for vector index operations
- [x] VectorIndexProcessor implementation following FTIndex pattern
- [x] Proper error handling and validation
- [x] Consistent locking mechanism

#### Meta Client (Completed)
- [x] CRUD methods for vector index operations
- [x] Cache-related methods aligned with FTIndex
- [x] vectorIndexMap_ in MetaData struct
- [x] Error handling and response processing

#### Testing (Completed)
- [x] Basic CRUD operation tests
- [x] Error case testing
- [x] Full workflow testing

### TODO

#### Documentation
- [ ] Add detailed comments explaining VectorIndex fields
- [ ] Document similarity metrics options and usage
- [ ] Provide example usage in comments
- [ ] Update user manual with vector index operations

#### Future Enhancements
1. Vector Type Support
   - [ ] Add native VECTOR type
   - [ ] Update schema validation for vector fields
   - [ ] Add dimension validation

2. Index Features
   - [ ] Add more similarity metrics
   - [ ] Support configurable index parameters
   - [ ] Add bulk operations
   - [ ] Add index status tracking

3. Performance
   - [ ] Add benchmarking tests
   - [ ] Optimize similarity search
   - [ ] Add caching strategies

## Compatibility

The implementation follows existing index patterns (particularly FTIndex) to maintain consistency and compatibility with existing code.

## Testing Plan

### Unit Tests (Completed)
- [x] CreateVectorIndexTest
- [x] CreateVectorIndexWithInvalidFieldTest
- [x] ListAndDropVectorIndexTest

### Future Tests
- [ ] Integration tests with storage service
- [ ] Compatibility tests

## Implementation Phases

1. Phase 1 (Completed)
   - Basic CRUD operations
   - Meta service implementation
   - Meta client implementation
   - Basic unit tests

2. Phase 2 (Planned)
   - Documentation
   - Native vector type support
   - Additional similarity metrics
   - Advanced index features

3. Phase 3 (Future)
   - Performance optimization
   - Advanced testing
   - Production hardening

## Notes

- Implementation closely follows FTIndex pattern for consistency
- Current implementation uses STRING type, will be updated to VECTOR type, this is not that important also due to we treat Vector as Index rather than Data.
- In Elasticsearch 7.17, Brute Force is the only KNN implementation(no ANN), so we cannot use KNN_QUERY but only script_score.
- Error handling matches existing NebulaGraph patterns
- Cache implementation aligns with other index types
