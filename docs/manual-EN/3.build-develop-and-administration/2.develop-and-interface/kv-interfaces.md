# KV interfaces

## Interface Demo

**Nebula Graph** storage provides key-value interfaces. Users can perform kv operations through the StorageClient. Please note that users still need to create space through console. Currently supported interfaces are Get and Put. The interfaces are as follows.

```cpp
    folly::SemiFuture<StorageRpcResponse<storage::cpp2::ExecResponse>> put(
      GraphSpaceID space,
      std::vector<nebula::cpp2::Pair> values,
      folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::GeneralResponse>> get(
      GraphSpaceID space,
      const std::vector<std::string>& keys,
      folly::EventBase* evb = nullptr);
```

Methods like remove, removeRange and scan will be provided later.

Interfaces usage are demonstrated as follows:

```cpp
// Put interface
std::vector<nebula::cpp2::Pair> pairs;
for (int32_t i = 0; i < 1000; i ++) {
    auto key = std::to_string(folly::Random::rand32(1000000000));
    auto value = std::to_string(folly::Random::rand32(1000000000));
    pairs.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                       std::move(key), std::move(value));
}

// Send requirements through StorageClient, the corresponding parameter is spaceId, the key-value pairs to put
auto future = storageClient->put(spaceId, std::move(pairs));
// Get results
auto resp = std::move(future).get();
```

```cpp
// Get interface
std::vector<std::string> keys;
for (auto& pair : pairs) {
    keys.emplace_back(pair.first);
}

// Send requirements through StorageClient, the corresponding parameter is spaceId, the keys to get
auto future = storageClient->get(spaceId, std::move(keys));
// Get results
auto resp = std::move(future).get()
```

### Processing Returned Results

Check the returned results of the rpc to examine if the corresponding operation runs successfully. In addition, since **Nebula Graph** storage shards data, if one partition fails, the error code is also returned. If any of the partition fails, the entire requirement fails (resp.succeeded() is false). But those succeed are still read/written.

Users can retry until all the requirements run successfully. Currently, auto retry is not supported by StorageClient. Users can decide whether to retry based on the error code.

```cpp
// Check if the call is successful
if (!resp.succeeded()) {
    LOG(ERROR) << "Operation Failed";
    return;
}

// Failed partitions and the corresponding error code
if (!resp.failedParts().empty()) {
    for (const auto& partEntry : resp.failedParts()) {
        LOG(ERROR) << "Operation Failed in " << partEntry.first << ", Code: "
                   << static_cast<int32_t>(partEntry.second);
    }
    return;
}
```

#### Read Values

For the Get interface, we need some more operations to get the corresponding values. **Nebula Graph** storage is a multi-copy based on Raft, and all read/written operations can only be sent to the leader of the corresponding partition. When a get request contains multiple keys across partitions, the Storage Client requests the keys from the Partition leader. Each rpc return is stored separately in an unordered_map, and the user is currently required to traverse these unordered_maps to check if the key exists. An example is as follows:

```cpp
// Examine whether the value corresponding to the key is in the returned result. If it exists, it is saved in the value.

bool found = false;
std::string value;
// resp.responses() it the results returned by the storage servers
for (const auto& result : resp.responses()) {
    // result.values is the returned key-value pairs of a certain storage server
    auto iter = result.values.find(key);
    if (iter != result.values.end()) {
        value = iter->second;
        found = true;
        break;
    }
}
```
