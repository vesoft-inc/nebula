/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"

#ifdef BUILT_WITH_SANITIZER

// Following hook functions are required by the sanitizer runtime library.
// So make them exported.
// Besides, keep these hooks outside the consideration of sanitizer
#define SANITIZER_HOOK_ATTRIBUTES                                               \
    __attribute__((visibility("default")))                                      \
    // __attribute__((no_sanitize("address", "thread", "undefined")))

extern "C" {

SANITIZER_HOOK_ATTRIBUTES
const char* __asan_default_options() {
    // You could add new or update existing options via ASAN_OPTIONS at runtime.
    return ""
           "fast_unwind_on_malloc=0 \n"
           "detect_stack_use_after_return=1 \n"
           "alloc_dealloc_mismatch=1 \n"
           "new_delete_type_mismatch=1 \n"
           "strict_init_order=1 \n"
           "intercept_tls_get_addr=1 \n"
           "symbolize_inline_frames=1 \n"
           "strict_string_checks=1 \n"
           "detect_container_overflow=1 \n"
           "strip_path_prefix=" NEBULA_STRINGIFY(NEBULA_HOME) "/ \n"
           "";
}


SANITIZER_HOOK_ATTRIBUTES
const char* __asan_default_suppressions() {
    return ""
           ""
           "";
}


SANITIZER_HOOK_ATTRIBUTES
const char* __lsan_default_options() {
    return ""
           ""
           "";
}


SANITIZER_HOOK_ATTRIBUTES
const char* __lsan_default_suppressions() {
    // NOTE  Don't add extra spaces around the suppression patterns, unless you intend to.
    return ""
           "leak:folly::SingletonVault::destroyInstances\n"
           "leak:__cxa_thread_atexit\n"
           "";
}


SANITIZER_HOOK_ATTRIBUTES
const char* __ubsan_default_options() {
    return "print_stacktrace=1 \n";
}

}   // extern "C"

#undef SANITIZER_HOOK_ATTRIBUTES

#endif
