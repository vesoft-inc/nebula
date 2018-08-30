/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef CPP_MACROS_H_
#define CPP_MACROS_H_

#define VE_MUST_USE_RESULT              __attribute__((warn_unused_result))
#define VE_DONT_OPTIMIZE                __attribute__((optimize("O0")))

#define VE_ALWAYS_INLINE                __attribute__((always_inline))
#define VE_ALWAYS_NO_INLINE             __attribute__((noinline))

#define VE_BEGIN_NO_OPTIMIZATION        _Pragma("GCC push_options") \
                                        _Pragma("GCC optimize(\"O0\")")
#define VE_END_NO_OPTIMIZATION          _Pragma("GCC pop_options")

#ifndef likely
#define likely(x)                       __builtin_expect((x),1)
#endif  // likely
#ifndef unlikely
#define unlikely(x)                     __builtin_expect((x),0)
#endif  // unlikely

#ifndef UNUSED
#define UNUSED(x) (void)(x)
#endif  // UNUSED

#ifndef COMPILER_BARRIER
#define COMPILER_BARRIER()              asm volatile ("":::"memory")
#endif  // COMPILER_BARRIER

#endif  // CPP_MACROS_H_
