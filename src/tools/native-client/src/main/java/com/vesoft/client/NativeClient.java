package com.vesoft.client;

import org.rocksdb.*;

import java.util.List;
import java.util.Objects;

public class NativeClient implements AutoCloseable {
    static {
        System.loadLibrary("client");
    }

    private SstFileWriter writer;

    public NativeClient(String path) throws RocksDBException {
        EnvOptions env = new EnvOptions();
        Options options = new Options();
        options.setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        new NativeClient(path, env, options);
    }

    public NativeClient(String path, EnvOptions env, Options options) throws RocksDBException {
        if (path == null || path.trim().length() == 0) {
            throw new IllegalArgumentException("File Path should not be null and empty");
        }
        writer = new SstFileWriter(env, options);
        writer.open(path);
    }

    public boolean addVertex(String key, Object[] values) {
        String value = encode(values);
        try {
            writer.put(key.getBytes(), value.getBytes());
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    public boolean addEdge(String key, Object[] values) {
        String value = encode(values);
        try {
            writer.put(key.getBytes(), value.getBytes());
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    public boolean deleteVertex(String key) { return delete(key); }

    public boolean deleteEdge(String key) { return delete(key); }

    private boolean delete(String key) {
        try {
            writer.delete(key.getBytes());
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    private native String encode(Object[] values);

    private boolean checkKey(byte[] key) {
        return Objects.isNull(key) || key.length == 0;
    }

    @Override
    public void close() throws Exception {
        writer.finish();
        writer.close();
    }

    public static void main(String[] args) throws RocksDBException {
        NativeClient client = new NativeClient("/tmp/data.sst");
        Object[] values = {false, 7, 1024L, 3.14F, 0.618, "darion.yaphet"}; // boolean int long float double String
        String result = client.encode(values);
        System.out.println("values : "+result +"  "+result.length());
    }
}
