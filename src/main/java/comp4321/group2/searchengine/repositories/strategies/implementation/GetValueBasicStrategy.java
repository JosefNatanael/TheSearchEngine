package comp4321.group2.searchengine.repositories.strategies.implementation;

import org.rocksdb.RocksDB;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetValueStrategy;

public class GetValueBasicStrategy implements IGetValueStrategy {

    @Override
    public byte[] getValue(byte[] key, RocksDB db) throws Exception {
        byte[] value = db.get(key);
        return value;
    }
}
