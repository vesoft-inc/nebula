/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

asm(
".pushsection \".debug_gdb_scripts\", \"MS\",@progbits,1\n"
".byte 4" "\n"
".ascii \"gdb.inlined-script\\n\"\n"
".ascii \"import glob\\n\"\n"
".ascii \"path = glob.glob('/usr/share/gcc-*/python')\\n\"\n"
".ascii \"sys.path = path + sys.path\\n\"\n"
".ascii \"try:\\n\"\n"
".ascii \"    from libstdcxx.v6 import register_libstdcxx_printers \\n\"\n"
".ascii \"    register_libstdcxx_printers(gdb.current_objfile()) \\n\"\n"
".ascii \"except ImportError: \\n\"\n"
".ascii \"    print('No pretty printer found for libstdc++') \\n\"\n"
".byte 0\n"
".popsection\n"
);
