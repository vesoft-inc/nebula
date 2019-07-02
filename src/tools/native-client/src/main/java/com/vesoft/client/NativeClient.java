/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(NativeClient.class.getName());

    static {
        System.loadLibrary("nebula_native_client");
    }

    public static class Pair {
        private String field;
        private String clazz;

        public Pair(String field, String clazz) {
            this.field = field;
            this.clazz = clazz;
        }

        public String getField() {
            return field;
        }

        public String getClazz() {
            return clazz;
        }

        @Override
        public String toString() {
            return "Pair{" + "field='" + field + '\'' + ", clazz=" + clazz + '}';
        }
    }

    private static final int PARTITION_ID = 4;
    private static final int VERTEX_ID = 8;
    private static final int TAG_ID = 4;
    private static final int TAG_VERSION = 8;
    private static final int EDGE_TYPE = 4;
    private static final int EDGE_RANKING = 8;
    private static final int EDGE_VERSION = 8;
    private static final int VERTEX_SIZE = PARTITION_ID + VERTEX_ID + TAG_ID + TAG_VERSION;
    private static final int EDGE_SIZE = PARTITION_ID + VERTEX_ID + EDGE_TYPE + EDGE_RANKING + VERTEX_ID + EDGE_VERSION;

    public static byte[] createEdgeKey(int partitionId, long srcId, int edgeType,
                                       long edgeRank, long dstId, long edgeVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(EDGE_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(partitionId);
        buffer.putLong(srcId);
        buffer.putInt(edgeType);
        buffer.putLong(edgeRank);
        buffer.putLong(dstId);
        buffer.putLong(edgeVersion);
        return buffer.array();
    }

    public static byte[] createVertexKey(int partitionId, long vertexId,
                                         int tagId, long tagVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(VERTEX_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(partitionId);
        buffer.putLong(vertexId);
        buffer.putInt(tagId);
        buffer.putLong(tagVersion);
        return buffer.array();
    }

    public static native byte[] encode(Object[] values);

    public static native Map<String, byte[]> decode(byte[] encoded, Pair[] fields);

    private boolean checkKey(String key) {
        return Objects.isNull(key) || key.length() == 0;
    }

    private boolean checkValues(Object[] values) {
        return Objects.isNull(values) || values.length == 0
                || Arrays.asList(values).contains(null);
    }
}
