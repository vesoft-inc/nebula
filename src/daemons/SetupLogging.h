/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef SETUPLOGGING_H
#define SETUPLOGGING_H

#include <string>

#include "common/base/Status.h"
/**
 * \param exe: program name.
 * \return wether successfully setupLogging.
 *
 */
nebula::Status setupLogging(const std::string &exe);
#endif
