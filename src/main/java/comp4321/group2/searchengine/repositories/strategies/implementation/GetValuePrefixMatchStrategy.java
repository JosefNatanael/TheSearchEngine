package comp4321.group2.searchengine.repositories.strategies.implementation;

import java.util.ArrayList;
import java.util.HashMap;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetValueStrategy;
import comp4321.group2.searchengine.utils.WordUtilities;

public class GetValuePrefixMatchStrategy implements IGetValueStrategy {

    @Override
    public HashMap<String, ArrayList<Integer>> getValue(byte[] prefix, RocksDB db) throws Exception {
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(false);
        ro.setPrefixSameAsStart(true);

        HashMap<String, ArrayList<Integer>> pageIdToWordLocs = new HashMap<String, ArrayList<Integer>>();
        RocksIterator iter = db.newIterator(ro);
        String keyStr, value;

        for (iter.seek(prefix); iter.isValid(); iter.next()) {
            keyStr = new String(iter.key());
            value = new String(iter.value());
            pageIdToWordLocs.put(
                WordUtilities.getPageIdFromKeyString(keyStr),
                WordUtilities.stringToIntegerArrayList(value)
            );
        }

        return pageIdToWordLocs;
    }
}