package comp4321.group2.searchengine.repositories.strategies.implementation;

import java.util.HashMap;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetAllEntriesStrategy;
import comp4321.group2.searchengine.utils.ByteIntUtilities;

public class GetAllStringIntegerEntriesStrategy implements IGetAllEntriesStrategy {

    @Override
    public HashMap<String, Integer> getAllEntries(RocksDB db) throws Exception {
        RocksIterator iter = db.newIterator();
        HashMap<String, Integer> result = new HashMap<String, Integer>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(new String(iter.key()), ByteIntUtilities.convertByteArrayToInt(iter.value()));
        }

        iter.close();
        return result;
    }
}
