package comp4321.group2.searchengine.repositories.strategies.interfaces;

import org.rocksdb.RocksDB;

public interface IGetAllEntriesStrategy {
    public Object getAllEntries(RocksDB db) throws Exception;
}
