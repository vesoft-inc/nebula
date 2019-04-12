/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/StorageFlags.h"

DEFINE_int32(storage_port, 44500, "Nebula Storage daemon's listen port");
DEFINE_string(storage_data_path, "", "Root data path, multi paths should be split by comma."
                                     "For rocksdb engine, one path one instance.");
DEFINE_string(storage_local_ip, "", "Local ip speicified for NetworkUtils::getLocalIP");
DEFINE_string(storage_pid_file, "pids/nebula-storaged.pid", "File to hold the storage process id");
DEFINE_bool(storage_daemonize, true, "Whether to run the process as a daemon");
