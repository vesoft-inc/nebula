package com.vesoft.client;

import org.rocksdb.*;

import java.util.List;
import java.util.Objects;

public class NativeClient /*implements Client*/ implements AutoCloseable {
    static {
        RocksDB.loadLibrary();
        System.loadLibrary("client");
    }

    private final SstFileWriter writer;

    public static class OptionsBuilder {
        private boolean createIfMissing;
        private boolean createMissingColumnFamilies;
        private long maxLogFileSize;

        public OptionsBuilder createIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }

        public OptionsBuilder createMissingColumnFamilies(boolean createMissingColumnFamilies) {
            this.createMissingColumnFamilies = createMissingColumnFamilies;
            return this;
        }

        public OptionsBuilder maxLogFileSize(long maxLogFileSize) {
            this.maxLogFileSize = maxLogFileSize;
            return this;
        }

        public Options build() {
            Options options = new Options();
            options
                    .setCreateIfMissing(createIfMissing)
                    .setCreateMissingColumnFamilies(createMissingColumnFamilies)
                    .setMaxLogFileSize(maxLogFileSize);
            return options;
        }
    }

    public NativeClient(String path) throws RocksDBException {

        if (path == null || path.trim().length() == 0) {
            throw new IllegalArgumentException("File Path should not null and empty");
        }

        EnvOptions env = new EnvOptions();
//        env.set

        OptionsBuilder builder = new OptionsBuilder();
        Options options = builder
                .createIfMissing(true)
                .createMissingColumnFamilies(true)
                .maxLogFileSize(1L)
                .build();

        writer = new SstFileWriter(env, options);
        writer.open(path);
    }

    //    @Override
    public boolean addVertex(long vertexID, int tagID, List<Object> fields) {
        byte[] key = new byte[0];
        byte[] value = new byte[0];
        try {
            writer.put(key, value);
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            return false;
        }
    }

    //    @Override
    public boolean addEdge(long srcID, long dstID, String type, long ranking, List<Object> fields) {
        byte[] key = new byte[0];
        byte[] value = new byte[0];

        if (checkKey(key)) {

        }

        try {
            writer.put(key, value);
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean deleteVertex() {
        try {
            writer.delete("key".getBytes());
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean deleteEdge() {
        try {
            writer.delete("key".getBytes());
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            return false;
        }
    }

    private native String encode(int value);

    //private native String decode();

    private boolean checkKey(byte[] key) {
        return Objects.isNull(key) || key.length == 0;
    }


    @Override
    public void close() throws Exception {
        writer.finish();
        writer.close();
    }

    public static void main(String[] args) {
        NativeClient client = null;
        try {
            client = new NativeClient("/tmp/data.sst");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        System.out.println(client.encode(0));
        //System.out.println(client.decode());
    }
}
