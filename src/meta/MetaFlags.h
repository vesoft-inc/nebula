/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METAFLAGS_H_
#define META_METAFLAGS_H_

#include "base/Base.h"

DECLARE_int32(meta_port);
DECLARE_bool(meta_daemonize);
DECLARE_string(meta_data_path);
DECLARE_string(meta_peers);
DECLARE_string(meta_local_ip);
DECLARE_string(meta_pid_file);

#endif  // META_METAFLAGS_H_
