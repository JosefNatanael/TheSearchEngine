package comp4321.group2.searchengine.repositories.strategies.interfaces;

import org.rocksdb.RocksDB;

public interface IGetValueStrategy {
    public Object getValue(byte[] key, RocksDB db) throws Exception;
}
