#include "storage/Shard.h"

namespace vesoft {
namespace vgraph {
namespace storage {

ResultCode Shard::get(const std::string& key, std::string& value) {
    VLOG(3) << "Get " << key << ", result " << value;
    return ResultCode::SUCCESSED;
}

ResultCode Shard::put(std::string key, std::string value) {
    VLOG(3) << "Put " << key << ", result " << value;
    return ResultCode::SUCCESSED;
}

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft

