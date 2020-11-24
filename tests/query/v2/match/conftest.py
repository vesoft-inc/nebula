# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest


@pytest.fixture
def like(request):
    def _like(src: str, dst: str):
        EDGES = request.cls.EDGES
        return EDGES[src+dst+'like'+str(0)]
    return _like


@pytest.fixture
def like_2hop(like):
    def _like_2hop(src: str, dst1: str, dst2: str):
        return [like(src, dst1), like(dst1, dst2)]
    return _like_2hop


@pytest.fixture
def like_2hop_start_with(like):
    def _like_2hop_start_with(start: str):
        def _like_2hop(dst1: str, dst2: str):
            return [like(start, dst1), like(dst1, dst2)]
        return _like_2hop
    return _like_2hop_start_with


@pytest.fixture
def like_3hop(like, like_2hop):
    def _like_3hop(src: str, dst1: str, dst2: str, dst3: str):
        return like_2hop(src, dst1, dst2) + [like(dst2, dst3)]
    return _like_3hop


@pytest.fixture
def like_3hop_start_with(like, like_2hop_start_with):
    def _like_3hop_start_with(start: str):
        like_2hop = like_2hop_start_with(start)

        def _like_3hop(dst1: str, dst2: str, dst3: str):
            return like_2hop(dst1, dst2) + [like(dst2, dst3)]
        return _like_3hop
    return _like_3hop_start_with


@pytest.fixture
def serve(request):
    def _serve(src: str, dst: str, rank=0):
        EDGES = request.cls.EDGES
        return EDGES[src+dst+'serve'+str(rank)]
    return _serve


@pytest.fixture
def serve_2hop(serve):
    def _serve_2hop(src: str, dst: str, dst2: str):
        return [serve(src, dst), serve(dst, dst2)]
    return _serve_2hop


@pytest.fixture
def serve_3hop(serve):
    def _serve_3hop(src: str, dst: str, dst2: str, dst3: str):
        return [serve(src, dst), serve(dst, dst2), serve(dst2, dst3)]
    return _serve_3hop
