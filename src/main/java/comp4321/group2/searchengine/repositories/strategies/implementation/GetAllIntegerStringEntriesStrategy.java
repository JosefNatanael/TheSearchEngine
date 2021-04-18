package comp4321.group2.searchengine.repositories.strategies.implementation;

import java.util.HashMap;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetAllEntriesStrategy;
import comp4321.group2.searchengine.utils.ByteIntUtilities;

public class GetAllIntegerStringEntriesStrategy implements IGetAllEntriesStrategy {

    @Override
    public HashMap<Integer, String> getAllEntries(RocksDB db) throws Exception {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, String> result = new HashMap<Integer, String>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), new String(iter.value()));
        }

        iter.close();
        return result;
    }
}
