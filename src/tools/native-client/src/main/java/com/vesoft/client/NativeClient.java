/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.primitives.UnsignedLong;
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
    private static final int EDGE_SIZE = PARTITION_ID + VERTEX_ID + EDGE_TYPE + EDGE_RANKING
        + VERTEX_ID + EDGE_VERSION;

    private static final int DATA_KEY_TYPE = 0x00000001;
    private static final int TAG_MASK      = 0xBFFFFFFF;
    private static final int EDGE_MASK     = 0x40000000;

    /**
     *
     * @param partitionId partition id that corresponding to the srcId
     * @param srcId src id
     * @param edgeType edge type in number
     * @param edgeRank edge ranking
     * @param dstId dest id
     * @param edgeVersion edge version
     * @return byte array
     */
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

    /**
     *
     * @param partitionId partition id that corresponding to the vertex id
     * @param vertexId vertex id
     * @param tagId tag id of vertex in number
     * @param tagVersion tag version
     * @return byte array
     */
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

    /**
     *
     * @param partionNum Total number of partitions
     * @param lat latitude that corresponding to the dstId
     * @param lng longitude that corresponding to the dstId
     * @param minCellLevel minimum cell levels for building the geo index
     * @param maxCellLevel maximum cell levels for building the geo index
     * @param edgeType edge type in number
     * @param edgeRank edge ranking
     * @param dstId destination id
     * @param edgeVersion edge version
     * @return list of the geo key in byte array
     */
    public static List<byte[]> createGeoKey(int partionNum,
                                            double lat, double lng, int minCellLevel, int maxCellLevel,
                                            int edgeType, long edgeRank, long dstId, long edgeVersion) {
        List<byte[]> result = new ArrayList<>();
        List<Long> cellIds = getS2IndexCells(lat, lng, minCellLevel, maxCellLevel);
        for (Long cellId : cellIds) {
            int partId = getPartitionId(cellId, partionNum);
            result.add(createEdgeKey(partId, cellId, edgeType, edgeRank, dstId, edgeVersion));
        }

        return result;
    }

    public static List<Long> getS2IndexCells(double lat, double lng, int minCellLevel, int maxCellLevel) {
        S2LatLng s2LatLng = S2LatLng.fromDegrees(lat, lng);
        S2CellId s2CellId = S2CellId.fromLatLng(s2LatLng);

        ArrayList<Long> cellIds = new ArrayList<>();
        for (int i = minCellLevel; i <= maxCellLevel; ++i) {
            cellIds.add(s2CellId.parent(i).id());
        }

        return cellIds;
    }

    /**
     *
     * @param id The vertexId
     * @param partionNum Total number of partitions
     * @return
     */
    public static int getPartitionId(long id, int partionNum) {
        // We'd better make this as an native interface
        // which will call the cpp func that generate the partid
        UnsignedLong unsignedId = UnsignedLong.valueOf(id);
        UnsignedLong ret = unsignedId.mod(UnsignedLong.valueOf(partionNum));

        return ret.intValue() + 1;
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
