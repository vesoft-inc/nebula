/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_VECTOR_VECTOREMBEDDINGFLAGS_H_
#define COMMON_PLUGIN_VECTOR_VECTOREMBEDDINGFLAGS_H_

#include "common/base/Base.h"
#include "gflags/gflags.h"

DECLARE_string(vector_embedding_url);
DECLARE_string(vector_embedding_auth_token);
DECLARE_int32(vector_embedding_timeout_ms);
DECLARE_int32(vector_embedding_retry_times);
DECLARE_int32(vector_embedding_batch_size);

#endif  // COMMON_PLUGIN_VECTOR_VECTOREMBEDDINGFLAGS_H_ 