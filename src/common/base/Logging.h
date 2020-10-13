/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_LOGGING_H_
#define COMMON_BASE_LOGGING_H_

#include <glog/logging.h>

#define _MINLOGLEVEL_GT_INFO \
    (FLAGS_minloglevel > google::GLOG_INFO)

#define _MINLOGLEVEL_GT_WARNING \
    (FLAGS_minloglevel > google::GLOG_WARNING)

#define _MINLOGLEVEL_GT_ERROR \
    (FLAGS_minloglevel > google::GLOG_ERROR)

#define _MINLOGLEVEL_GT_FATAL \
    (FLAGS_minloglevel > google::GLOG_FATAL)

#ifdef NDEBUG
// Treat DFATAL as ERROR in non-debug mode
#define _MINLOGLEVEL_GT_DFATAL \
    (FLAGS_minloglevel > google::GLOG_ERROR)
#else
// Treat DFATAL as FATAL in debug mode
#define _MINLOGLEVEL_GT_DFATAL \
    (FLAGS_minloglevel > google::GLOG_FATAL)
#endif


/*****************************************
 *
 * Normal loggings
 *
 ****************************************/
#define _LOG_INFO \
    _MINLOGLEVEL_GT_INFO \
        ? (void) 0 \
        : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_INFO.stream()

#define _LOG_WARNING \
    _MINLOGLEVEL_GT_WARNING \
        ? (void) 0 \
        : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_WARNING.stream()

#define _LOG_ERROR \
    _MINLOGLEVEL_GT_ERROR \
        ? (void) 0 \
        : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ERROR.stream()

#define _LOG_FATAL COMPACT_GOOGLE_LOG_FATAL.stream()

#define _LOG_DFATAL \
    _MINLOGLEVEL_GT_DFATAL \
        ? (void) 0 \
        : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_DFATAL.stream()

#undef LOG
#define LOG(severity) _LOG_ ## severity

#undef PLOG
#define PLOG(severity) \
  _MINLOGLEVEL_GT_ ## severity \
    ? (void) 0 \
    : google::LogMessageVoidify() & GOOGLE_PLOG(severity, 0).stream()

#undef SYSLOG
#define SYSLOG(severity) \
  _MINLOGLEVEL_GT_ ## severity \
    ? (void) 0 \
    : google::LogMessageVoidify() & SYSLOG_ ## severity(0).stream()

#undef LOG_TO_STRING
#define LOG_TO_STRING(severity, message) \
  _MINLOGLEVEL_GT_ ## severity \
    ? (void) 0 \
    : google::LogMessageVoidify() & \
      LOG_TO_STRING_##severity(static_cast<string*>(message)).stream()

#undef LOG_STRING
#define LOG_STRING(severity, outvec) \
  _MINLOGLEVEL_GT_ ## severity \
    ? (void) 0 \
    : google::LogMessageVoidify() & \
      LOG_TO_STRING_##severity(static_cast<std::vector<std::string>*>(outvec)).stream()


/*****************************************
 *
 * Conditional loggings
 *
 ****************************************/
#undef LOG_IF
#define LOG_IF(severity, condition) \
  (!(condition) || _MINLOGLEVEL_GT_ ## severity) \
    ? (void) 0 \
    : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ ## severity.stream()

#undef PLOG_IF
#define PLOG_IF(severity, condition) \
  (!(condition) || _MINLOGLEVEL_GT_ ## severity) \
    ? (void) 0 \
    : google::LogMessageVoidify() & GOOGLE_PLOG(severity, 0).stream()

#undef SYSLOG_IF
#define SYSLOG_IF(severity, condition) \
  (!(condition) || _MINLOGLEVEL_GT_ ## severity) \
    ? (void) 0 \
    : google::LogMessageVoidify() & SYSLOG_ ## severity(0).stream()


/*****************************************
 *
 * Sampling loggings
 *
 ****************************************/
#undef SOME_KIND_OF_LOG_EVERY_N
#define SOME_KIND_OF_LOG_EVERY_N(severity, n, what_to_do) \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0; \
  ++LOG_OCCURRENCES; \
  if (++LOG_OCCURRENCES_MOD_N > n) LOG_OCCURRENCES_MOD_N -= n; \
  if (!(_MINLOGLEVEL_GT_ ## severity) && LOG_OCCURRENCES_MOD_N == 1) \
    google::LogMessage( \
        __FILE__, __LINE__, google::GLOG_ ## severity, LOG_OCCURRENCES, &what_to_do).stream()

#undef SOME_KIND_OF_LOG_IF_EVERY_N
#define SOME_KIND_OF_LOG_IF_EVERY_N(severity, condition, n, what_to_do) \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0; \
  ++LOG_OCCURRENCES; \
  if (++LOG_OCCURRENCES_MOD_N > n) LOG_OCCURRENCES_MOD_N -= n; \
  if (!(_MINLOGLEVEL_GT_ ## severity) && (condition) && LOG_OCCURRENCES_MOD_N == 1) \
    google::LogMessage( \
        __FILE__, __LINE__, google::GLOG_ ## severity, LOG_OCCURRENCES, &what_to_do).stream()

#undef SOME_KIND_OF_PLOG_EVERY_N
#define SOME_KIND_OF_PLOG_EVERY_N(severity, n, what_to_do) \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0; \
  ++LOG_OCCURRENCES; \
  if (++LOG_OCCURRENCES_MOD_N > n) LOG_OCCURRENCES_MOD_N -= n; \
  if (!(_MINLOGLEVEL_GT_ ## severity) && LOG_OCCURRENCES_MOD_N == 1) \
    google::ErrnoLogMessage( \
        __FILE__, __LINE__, google::GLOG_ ## severity, LOG_OCCURRENCES, &what_to_do).stream()

#undef SOME_KIND_OF_LOG_FIRST_N
#define SOME_KIND_OF_LOG_FIRST_N(severity, n, what_to_do) \
  static int LOG_OCCURRENCES = 0; \
  if (LOG_OCCURRENCES <= n) \
    ++LOG_OCCURRENCES; \
  if (!(_MINLOGLEVEL_GT_ ## severity) && LOG_OCCURRENCES <= n) \
    google::LogMessage( \
        __FILE__, __LINE__, google::GLOG_ ## severity, LOG_OCCURRENCES, &what_to_do).stream()


#undef LOG_EVERY_N
#define LOG_EVERY_N(severity, n) \
  SOME_KIND_OF_LOG_EVERY_N(severity, (n), google::LogMessage::SendToLog)

#undef LOG_IF_EVERY_N
#define LOG_IF_EVERY_N(severity, condition, n) \
  SOME_KIND_OF_LOG_IF_EVERY_N(severity, (condition), (n), google::LogMessage::SendToLog)

#undef PLOG_EVERY_N
#define PLOG_EVERY_N(severity, n) \
  SOME_KIND_OF_PLOG_EVERY_N(severity, (n), google::LogMessage::SendToLog)

#undef SYSLOG_EVERY_N
#define SYSLOG_EVERY_N(severity, n) \
  SOME_KIND_OF_LOG_EVERY_N(severity, (n), google::LogMessage::SendToSyslogAndLog)

#undef LOG_FIRST_N
#define LOG_FIRST_N(severity, n) \
  SOME_KIND_OF_LOG_FIRST_N(severity, (n), google::LogMessage::SendToLog)


/*****************************************
 *
 * Debugging loggings
 *
 * Only works when compiled with NDEBUG=1
 *
 ****************************************/
#undef DLOG
#ifndef NDEBUG
#define DLOG(severity) LOG(severity)
#else
#define DLOG(severity) \
    true ? (void) 0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ ## severity.stream()
#endif

#undef DLOG_IF
#ifndef NDEBUG
#define DLOG_IF(severity, condition) LOG_IF(severity, condition)
#else
#define DLOG_IF(severity, condition) \
    true ? (void) 0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ ## severity.stream()
#endif

#undef DLOG_EVERY_N
#ifndef NDEBUG
#define DLOG_EVERY_N(severity, n) LOG_EVERY_N(severity, n)
#else
#define DLOG_EVERY_N(severity, n) \
    true ? (void) 0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ ## severity.stream()
#endif

#undef DLOG_IF_EVERY_N
#ifndef NDEBUG
#define DLOG_IF_EVERY_N(severity, condition, n) LOG_IF_EVERY_N(severity, condition, n)
#else
#define DLOG_IF_EVERY_N(severity, condition, n) \
    true ? (void) 0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ ## severity.stream()
#endif

#endif  // COMMON_BASE_LOGGING_H_

