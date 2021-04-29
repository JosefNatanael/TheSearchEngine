package comp4321.group2.searchengine.repositories;

import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;


public class PageIdToParentIds {

    private static RocksDB db;

    public static void connect(boolean isProduction) throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.useFixedLengthPrefixExtractor(8);

        // create and open the database
        // create the DB if directory does not exist, then open the DB
        File directory = isProduction ? new File("./src/main/java/tables/PageIdToParentIds") : new File("./src/test/java/tables/PageIdToParentIds");
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

    public static void addEntry(int pageId, ArrayList<Integer> parentIds) throws RocksDBException {

        byte[] key = ByteIntUtilities.convertIntToByteArray(pageId);
        byte[] values = WordUtilities.arrayListToString(parentIds).getBytes();

        db.put(key, values);
    }

    public static void delEntry(int pageId) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(ByteIntUtilities.convertIntToByteArray(pageId));
    }

    public static ArrayList<Integer> getValue(int pageId) throws RocksDBException {
        byte[] value = db.get(ByteIntUtilities.convertIntToByteArray(pageId));

        return WordUtilities.stringToIntegerArrayList(new String(value));
    }

    /**
     * Get all the result pairs
     */
    public static HashMap<Integer, ArrayList<Integer>> getAll() {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), WordUtilities.stringToIntegerArrayList(new String(iter.value())));
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
            System.out.println(ByteIntUtilities.convertByteArrayToInt(iter.key()) + "\t=\t" + WordUtilities.stringToIntegerArrayList(new String(iter.value())));
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
}
