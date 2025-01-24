---
name: Vector Index Support
about: Basic vector similarity search using Elasticsearch KNN

---

# Summary

Add basic vector similarity search support to NebulaGraph by leveraging Elasticsearch's KNN capabilities and existing ES integration infrastructure.

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
WITH DIMENSION = 128, MODEL = "remote_model_endpoint";
```

## Vector Search 

```ngql
-- Basic vector similarity search
LOOKUP ON tag_name
WHERE VECTOR_QUERY(index_name, "text to convert to vector") 
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

### Phase 2 (In Progress)
1. Storage Service:
   - [ ] Extend ESListener for vector conversion
   - [ ] Store vectors in ES dense_vector field
   - [ ] Basic error handling

2. Query Processing:
   - [ ] Add VECTOR_QUERY predicate
   - [ ] Basic KNN search via ES
   - [ ] Reuse existing score handling

3. Documentation:
   - [ ] Add detailed comments explaining VectorIndex fields
   - [ ] Document similarity metrics options and usage
   - [ ] Provide example usage in comments
   - [ ] Update user manual with vector index operations

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
- [ ] Performance tests
- [ ] Stress tests
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
- Error handling matches existing NebulaGraph patterns
- Cache implementation aligns with other index types
