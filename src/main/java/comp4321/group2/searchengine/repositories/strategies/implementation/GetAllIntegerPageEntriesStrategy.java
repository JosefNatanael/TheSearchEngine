package comp4321.group2.searchengine.repositories.strategies.implementation;

import java.util.HashMap;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetAllEntriesStrategy;
import comp4321.group2.searchengine.utils.ByteIntUtilities;

public class GetAllIntegerPageEntriesStrategy implements IGetAllEntriesStrategy {

    @Override
    public HashMap<Integer, Page> getAllEntries(RocksDB db) throws Exception {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, Page> result = new HashMap<>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), Page.deserialize(iter.value()));
        }

        iter.close();
        return result;
    }
}
