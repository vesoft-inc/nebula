# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

def __bytes2ul(b):
    return int.from_bytes(b, byteorder='little', signed=False)


def mmh2(bstr, seed=0xc70f6907, signed=True):
    MASK = 2 ** 64 - 1
    size = len(bstr)
    m = 0xc6a4a7935bd1e995
    r = 47
    h = seed ^ (size * m & MASK)
    end = size & (0xfffffff8)
    for pos in range(0, end, 8):
        k = __bytes2ul(bstr[pos:pos+8])
        k = k * m & MASK
        k = k ^ (k >> r)
        k = k * m & MASK;
        h = h ^ k
        h = h * m & MASK

    left = size & 0x7
    if left >= 7:
        h = h ^ (bstr[end+6] << 48)
    if left >= 6:
        h = h ^ (bstr[end+5] << 40)
    if left >= 5:
        h = h ^ (bstr[end+4] << 32)
    if left >= 4:
        h = h ^ (bstr[end+3] << 24)
    if left >= 3:
        h = h ^ (bstr[end+2] << 16)
    if left >= 2:
        h = h ^ (bstr[end+1] << 8)
    if left >= 1:
        h = h ^ bstr[end+0]
        h = h * m & MASK

    h = h ^ (h >> r)
    h = h * m & MASK
    h = h ^ (h >> r)

    if signed:
        h = h | (-(h & 0x8000000000000000))

    return h


if __name__ == '__main__':
    assert mmh2(b'hello') == 2762169579135187400
    assert mmh2(b'World') == -295471233978816215
    assert mmh2(b'Hello World') == 2146989006636459346
    assert mmh2(b'Hello Wo') == -821961639117166431
