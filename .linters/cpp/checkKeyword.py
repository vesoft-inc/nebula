# signout--coding:utf-8--

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import re
import sys

PASER_FILE_PATH = 'src/parser/parser.yy'
SCANNER_FILE_PATH = 'src/parser/scanner.lex'

reserved_key_words = [
    'KW_GO',
    'KW_AS',
    'KW_TO',
    'KW_OR',
    'KW_AND',
    'KW_XOR',
    'KW_USE',
    'KW_SET',
    'KW_FROM',
    'KW_WHERE',
    'KW_MATCH',
    'KW_INSERT',
    'KW_YIELD',
    'KW_RETURN',
    'KW_DESCRIBE',
    'KW_DESC',
    'KW_VERTEX',
    'KW_EDGE',
    'KW_EDGES',
    'KW_UPDATE',
    'KW_UPSERT',
    'KW_WHEN',
    'KW_DELETE',
    'KW_FIND',
    'KW_LOOKUP',
    'KW_ALTER',
    'KW_STEPS',
    'KW_OVER',
    'KW_UPTO',
    'KW_REVERSELY',
    'KW_INDEX',
    'KW_INDEXES',
    'KW_REBUILD',
    'KW_BOOL',
    'KW_INT8',
    'KW_INT16',
    'KW_INT32',
    'KW_INT64',
    'KW_INT',
    'KW_FLOAT',
    'KW_DOUBLE',
    'KW_STRING',
    'KW_FIXED_STRING',
    'KW_TIMESTAMP',
    'KW_DATE',
    'KW_TIME',
    'KW_DATETIME',
    'KW_VID_SIZE',
    'KW_TAG',
    'KW_TAGS',
    'KW_UNION',
    'KW_INTERSECT',
    'KW_MINUS',
    'KW_NO',
    'KW_OVERWRITE',
    'KW_SHOW',
    'KW_ADD',
    'KW_CREATE',
    'KW_DROP',
    'KW_REMOVE',
    'KW_IF',
    'KW_NOT',
    'KW_EXISTS',
    'KW_WITH',
    'KW_CHANGE',
    'KW_GRANT',
    'KW_REVOKE',
    'KW_ON',
    'KW_BY',
    'KW_IN',
    'KW_NOT_IN',
    'KW_DOWNLOAD',
    'KW_GET',
    'KW_OF',
    'KW_ORDER',
    'KW_INGEST',
    'KW_COMPACT',
    'KW_FLUSH',
    'KW_SUBMIT',
    'KW_ASC',
    'KW_ASCENDING',
    'KW_DESCENDING',
    'KW_DISTINCT',
    'KW_FETCH',
    'KW_PROP',
    'KW_BALANCE',
    'KW_STOP',
    'KW_GROUP',
    'KW_IS',
    'KW_NULL',
    'KW_FORCE',
    'KW_RECOVER',
    'KW_EXPLAIN',
    'KW_UNWIND',
    'KW_CASE',
]


def get_unreserved_keyword(file_path):
    parser_file = open(file_path)
    flag = 0
    unreserved_key_words = []
    for line in parser_file.readlines():
        if line.strip() == 'unreserved_keyword':
            flag = 1
            continue
        if flag == 1:
            if line.strip() == ';':
                break
            unreserved_key_words.append(re.sub('\\s+[:|]\\s+(\\w+)\\s+.*', '\\1', line).strip())
            continue

    parser_file.close()
    return unreserved_key_words


if __name__ == '__main__':
    cmd = 'git diff --diff-filter=ACMRTUXB HEAD -p ' + SCANNER_FILE_PATH + '|grep "^+"|grep -v "^+++"|grep "KW_"'
    content = os.popen(cmd)
    keywords=[]
    for line in  content.readlines():
        keyword = re.sub('.*(KW_\\w+)\s*;.*','\\1',line.strip())
        keywords.append(keyword)

    if len(keywords) == 0:
        exit(0)
    unreserved_key_words = get_unreserved_keyword(PASER_FILE_PATH)
    new_key_words = [word for word in keywords if word not in reserved_key_words]
    if len(new_key_words) == 0:
        exit(0)
    result = [word for word in new_key_words if word not in unreserved_key_words]
    if len(result) == 0:
        exit(0)
    print('Keywords \"{}\" in src/parser/scanner.lex are not in the unreserved keyword list.'.format(result))
    print('Please add them to the unreserved keyword in the src/parser/parser.yy.')
    exit(1)
