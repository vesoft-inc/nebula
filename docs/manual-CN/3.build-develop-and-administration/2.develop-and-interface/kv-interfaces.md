# KV 接口

## 接口示例

nebula storage 提供 key-value 接口，用户可以通过 StorageClient 进行 kv 的相关操作，请注意用户仍然需要通过 console 来创建 space。目前支持的接口有 Get 和 Put，接口如下。

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

后续将提供 remove，removeRange 以及 scan 的方法。

下面结合示例说明 kv 接口的使用方法：

```cpp
// Put接口
std::vector<nebula::cpp2::Pair> pairs;
for (int32_t i = 0; i < 1000; i ++) {
    auto key = std::to_string(folly::Random::rand32(1000000000));
    auto value = std::to_string(folly::Random::rand32(1000000000));
    pairs.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                       std::move(key), std::move(value));
}

// 通过StorageClient发送请求，相应的参数为spaceId，以及写入的key-value pairs
auto future = storageClient->put(spaceId, std::move(pairs));
// 获取结果
auto resp = std::move(future).get();
```

```cpp
// Get接口
std::vector<std::string> keys;
for (auto& pair : pairs) {
    keys.emplace_back(pair.first);
}

// 通过StorageClient发送请求，相应的参数为spaceId，以及要获取的keys
auto future = storageClient->get(spaceId, std::move(keys));
// 获取结果
auto resp = std::move(future).get()
```

## 处理返回结果

用户可以通过检查 rpc 返回结果查看相应操作是否成功。此外由于每个 nebula storage 中都对数据进行了分片，因此如果对应的 Partition 失败了，也会返回每个失败的 Partition 的错误码。若任意一个 Partition 失败，则整个请求失败(resp.succeeded()为 false)，但是其他成功的 Partition 仍然会成功写入或读取。

用户可以进行重试，直至所有请求都成功。目前 StorageClient 不支持自动重试，用户可以根据错误码决定是否进行重试。

```cpp
// 判断调用是否成功
if (!resp.succeeded()) {
    LOG(ERROR) << "Operation Failed";
    return;
}

// 失败的Partition以及相应的错误码
if (!resp.failedParts().empty()) {
    for (const auto& partEntry : resp.failedParts()) {
        LOG(ERROR) << "Operation Failed in " << partEntry.first << ", Code: "
                   << static_cast<int32_t>(partEntry.second);
    }
    return;
}
```

### 读取返回值

对于 Get 接口，用户需要一些操作来获取相应的返回值。Nebula storage 是基于 Raft 的多副本，所有读写操作只能发送给对应 partition 的 leader。当一个 rpc 请求包含了多个跨 partition 的 get 时，Storage Client 会给访问这些 key 所对应的 Partition leader。每个 rpc 返回都单独保存在一个 unordered_map 中，目前还需要用户在这些 unordered_map 中遍历查找 key 是否存在。示例如下：

```cpp
// 查找key对应的value是否在返回结果中，如果存在，则保存在value中
bool found = false;
std::string value;
// resp.responses()中是多个storage server返回的结果
for (const auto& result : resp.responses()) {
    // result.values即为某个storage server返回的key-value paris
    auto iter = result.values.find(key);
    if (iter != result.values.end()) {
        value = iter->second;
        found = true;
        break;
    }
}
```
