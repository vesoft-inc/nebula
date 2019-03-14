package com.vesoft.client;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

public class NativeClient implements AutoCloseable {
    static {
        System.loadLibrary("nebula_native_client");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeClient.class.getName());

    private static final int PARTITION_ID = 4;
    private static final int VERTEX_ID = 8;
    private static final int TAG_ID = 4;
    private static final int TAG_VERSION = 8;
    private static final int EDGE_TYPE = 4;
    private static final int EDGE_RANKING = 8;
    private static final int EDGE_VERSION = 8;
    private static final int VERTEX_SIZE = PARTITION_ID + VERTEX_ID + TAG_ID + TAG_VERSION;
    private static final int EDGE_SIZE = PARTITION_ID + VERTEX_ID + EDGE_TYPE + EDGE_RANKING + VERTEX_ID + EDGE_VERSION;

    private SstFileWriter writer;

    public NativeClient(String path) {
        EnvOptions env = new EnvOptions();
        Options options = new Options();
        options.setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        new NativeClient(path, env, options);
    }

    public NativeClient(String path, EnvOptions env, Options options) {
        if (path == null || path.trim().length() == 0) {
            throw new IllegalArgumentException("File Path should not be null and empty");
        }
        writer = new SstFileWriter(env, options);
        try {
            writer.open(path);
        } catch (RocksDBException e) {
            LOGGER.error("SstFileWriter Open Failed {}", e.getMessage());
        }
    }

    public boolean addVertex(String key, Object[] values) {
        if (checkKey(key) || checkValues(values)) {
            throw new IllegalArgumentException("Add Vertex key and value should not null");
        }

        byte[] value = encode(values);
        try {
            writer.put(key.getBytes(), value);
            return true;
        } catch (RocksDBException e) {
            LOGGER.error("AddVertex Failed {}", e.getMessage());
            return false;
        }
    }

    public boolean addEdge(String key, Object[] values) {
        if (checkKey(key) || checkValues(values)) {
            throw new IllegalArgumentException("Add Vertex key and value should not null");
        }

        byte[] value = encode(values);
        try {
            writer.put(key.getBytes(), value);
            return true;
        } catch (RocksDBException e) {
            LOGGER.error("AddEdge Failed {}", e.getMessage());
            return false;
        }
    }

    public boolean deleteVertex(String key) { return delete(key); }

    public boolean deleteEdge(String key) { return delete(key); }

    private boolean delete(String key) {
        if (checkKey(key)) {
            throw new IllegalArgumentException("Add Vertex key and value should not null");
        }

        try {
            writer.delete(key.getBytes());
            return true;
        } catch (RocksDBException e) {
            LOGGER.error("Delete Failed {}", e.getMessage());
            return false;
        }
    }

    public static byte[] createEdgeKey(int partitionID, long srcID, int edgeType,
                                       long edgeRank, long dstID, long edgeVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(EDGE_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(partitionID);
        buffer.putLong(srcID);
        buffer.putInt(edgeType);
        buffer.putLong(edgeRank);
        buffer.putLong(dstID);
        buffer.putLong(edgeVersion);
        return buffer.array();
    }

    public static byte[] createVertexKey(int partitionID, long vertexID,
                                         int tagID, long tagVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(VERTEX_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(partitionID);
        buffer.putLong(vertexID);
        buffer.putInt(tagID);
        buffer.putLong(tagVersion);
        return buffer.array();
    }

    private static native byte[] encode(Object[] values);

    private boolean checkKey(String key) {
        return Objects.isNull(key) || key.length() == 0;
    }

    private boolean checkValues(Object[] values) {
        return Objects.isNull(values) || values.length == 0 ||
                Arrays.asList(values).contains(null);
    }

    @Override
    public void close() throws Exception {
        writer.finish();
        writer.close();
    }

    public static void main(String[] args) throws RocksDBException {
        Object[] values = {
                false,
                7,
                1024L,
                3.14F,
                0.618,
                "Hello World!".getBytes()
        };
        byte[] result = encode(values);
        System.out.println("values : " + new String(result) + " Size " + result.length);
    }
}
