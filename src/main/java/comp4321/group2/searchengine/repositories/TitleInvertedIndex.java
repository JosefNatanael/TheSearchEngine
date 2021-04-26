package comp4321.group2.searchengine.repositories;

import org.rocksdb.RocksDB;

public class TitleInvertedIndex extends RocksDBRepo {
    public TitleInvertedIndex(RocksDB db) {
        super(db);
    }


}
