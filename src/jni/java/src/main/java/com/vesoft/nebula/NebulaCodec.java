/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaCodec {
    private static final Logger LOGGER = LoggerFactory.getLogger(NebulaCodec.class.getName());

    static {
        try {
            System.loadLibrary("nebula_codec");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } catch (Error e) {
            LOGGER.error(e.getMessage(), e);
        }
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

    private static final int PARTITION_ID_SIZE = 4;
    private static final int VERTEX_ID_SIZE = 8;
    private static final int TAG_ID_SIZE = 4;
    private static final int TAG_VERSION_SIZE = 8;
    private static final int EDGE_TYPE_SIZE = 4;
    private static final int EDGE_RANKING_SIZE = 8;
    private static final int EDGE_VERSION_SIZE = 8;
    private static final int VERTEX_SIZE = PARTITION_ID_SIZE + VERTEX_ID_SIZE
        + TAG_ID_SIZE + TAG_VERSION_SIZE;
    private static final int EDGE_SIZE = PARTITION_ID_SIZE + VERTEX_ID_SIZE
        + EDGE_TYPE_SIZE + EDGE_RANKING_SIZE + VERTEX_ID_SIZE + EDGE_VERSION_SIZE;

    private static final int DATA_KEY_TYPE = 0x00000001;
    private static final int TAG_MASK      = 0xBFFFFFFF;
    private static final int EDGE_MASK     = 0x40000000;

    public static byte[] createEdgeKey(int partitionId, long srcId, int edgeType,
                                       long edgeRank, long dstId, long edgeVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(EDGE_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        partitionId = (partitionId << 8) | DATA_KEY_TYPE;
        buffer.putInt(partitionId);
        buffer.putLong(srcId);
        edgeType |= EDGE_MASK;
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
        partitionId = (partitionId << 8) | DATA_KEY_TYPE;
        buffer.putInt(partitionId);
        buffer.putLong(vertexId);
        tagId &= TAG_MASK;
        buffer.putInt(tagId);
        buffer.putLong(tagVersion);
        return buffer.array();
    }

    public static native byte[] encode(Object[] values);

    public static native List<byte[]> decode(byte[] encoded, Pair[] fields, long version);

    private boolean checkKey(String key) {
        return Objects.isNull(key) || key.length() == 0;
    }

    private boolean checkValues(Object[] values) {
        return Objects.isNull(values) || values.length == 0
                || Arrays.asList(values).contains(null);
    }
}
