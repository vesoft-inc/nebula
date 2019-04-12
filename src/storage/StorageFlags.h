/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_STORAGEFLAGS_H_
#define STORAGE_STORAGEFLAGS_H_

#include "base/Base.h"

DECLARE_int32(storage_port);
DECLARE_string(storage_data_path);
DECLARE_string(storage_local_ip);
DECLARE_string(storage_pid_file);
DECLARE_bool(storage_daemonize);

#endif  // STORAGE_STORAGEFLAGS_H_
