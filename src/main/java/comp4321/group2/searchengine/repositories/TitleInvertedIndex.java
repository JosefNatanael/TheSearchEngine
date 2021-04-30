package comp4321.group2.searchengine.repositories;

import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class TitleInvertedIndex {

    private static RocksDB db;

    public static void connect(boolean isProduction) throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.useFixedLengthPrefixExtractor(8);

        // create and open the database
        // create the DB if directory does not exist, then open the DB
        File directory = isProduction ? new File("./src/main/java/tables/TitleInvertedIndex") : new File("./src/test/java/tables/TitleInvertedIndex");
        String dbPath = directory.getAbsolutePath();
        if (!directory.exists()) {
            directory.mkdir();
        }
        db = RocksDB.open(options, dbPath);
    }

    public static void closeConnection() {
        if (db != null) {
            db.close();
        }
    }

    public static void addEntry(byte[] key, byte[] values) throws RocksDBException {
        db.put(key, values);
    }

    public static void delEntry(String entry) throws RocksDBException {
        // Delete the entry and its list from the hashtable
        db.delete(entry.getBytes());
    }

    //prefix match
    public static HashMap<Integer, ArrayList<Integer>> getValue(byte[] prefix) {
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(false);
        ro.setPrefixSameAsStart(true);

        HashMap<Integer, ArrayList<Integer>> pageIdToTitleWordLocs = new HashMap<>();
        RocksIterator iter = db.newIterator(ro);
        String key, value;

        for (iter.seek(prefix); iter.isValid(); iter.next()) {
            key = new String(iter.key());
            value = new String(iter.value());
            pageIdToTitleWordLocs.put(
                Integer.parseInt(WordUtilities.getSuffixFromKeyString(key)),
                WordUtilities.stringToIntegerArrayList(value)
            );
        }

        iter.close();
        return pageIdToTitleWordLocs;
    }

    public static ArrayList<Integer> getValueByKey(byte[] key) throws RocksDBException {
        byte[] value = db.get(key);
        return WordUtilities.stringToIntegerArrayList(new String(value));
    }


    public static ArrayList<Integer> getPageIds(byte[] prefix) {
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(false);
        ro.setPrefixSameAsStart(true);

        ArrayList<Integer> pageIds = new ArrayList<>();
        RocksIterator iter = db.newIterator(ro);

        for (iter.seek(prefix); iter.isValid(); iter.next()) {
            String key = new String(iter.key());
            String pageId = WordUtilities.getSuffixFromKeyString(key);
            pageIds.add(Integer.parseInt(pageId));
        }

        return pageIds;
    }

    /**
     * Get all the result pairs
     */
    public static HashMap<Integer, String> getAll() {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, String> result = new HashMap<>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), new String(iter.value()));
        }

        iter.close();
        return result;
    }

    /**
     * Print all the data in the DB to the console
     */
    public static void printAll() {
        // Print all the data in the hashtable
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + "\t=\t" + new String(iter.value()));
        }

        iter.close();
    }

    /**
     * Delete all the data in the DB
     */
    public static void deleteAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            db.delete(iter.key());
        }

        iter.close();
    }

    /**
     * Creates NEW entries in the database in batch.
     * Table: <invertedIndexKey: byte array, locations: int array>
     */
    public static void createEntriesInBatch(Map<byte[], ArrayList<Integer>> table)
        throws RocksDBException {

        byte[] keyword;
        ArrayList<Integer> locations;

        for (Entry<byte[], ArrayList<Integer>> it : table.entrySet()) {
            keyword = it.getKey();
            locations = it.getValue();

            // Step 2: Put into write batch
            db.put(keyword, WordUtilities.arrayListToString(locations).getBytes());
        }
    }
}

