package comp4321.group2.searchengine.repositories;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.WordUtilities;


public class ForwardIndex {

    private static RocksDB db;
    private static Options options;

    public static void connect() throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        options = new Options();
        options.setCreateIfMissing(true);
        options.useFixedLengthPrefixExtractor(8);

        // create and open the database
        // create the DB if directory does not exist, then open the DB
        File directory = new File("./src/main/java/tables/ForwardFile");
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

    public static void addEntry(int pageId, ArrayList<Integer> wordIds) throws RocksDBException {

        byte[] key = ByteIntUtilities.convertIntToByteArray(pageId);
        byte[] values = WordUtilities.arrayListToString(wordIds).getBytes();

        db.put(key, values);
    }

    //prefix match
    public static void delEntry(int pageId) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(ByteIntUtilities.convertIntToByteArray(pageId));
    }

    public static ArrayList<Integer> getValue(int pageId) throws RocksDBException {
        byte[] value = db.get(ByteIntUtilities.convertIntToByteArray(pageId));

        ArrayList<Integer> result = WordUtilities.stringToIntegerArrayList(new String(value));

        return result;
    }

    /**
     * Get all the result pairs
     *
     * @throws RocksDBException
     */
    public static HashMap<Integer, ArrayList<Integer>> getAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<Integer, ArrayList<Integer>>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), WordUtilities.stringToIntegerArrayList(new String(iter.value())));
        }

        iter.close();
        return result;
    }

    /**
     * Print all the data in the DB to the console
     *
     * @throws RocksDBException
     */
    public static void printAll() throws RocksDBException {
        // Print all the data in the hashtable
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(ByteIntUtilities.convertByteArrayToInt(iter.key()) + "\t=\t" + WordUtilities.stringToIntegerArrayList(new String(iter.value())));
        }

        iter.close();
    }

    /**
     * Delete all the data in the DB
     *
     * @throws RocksDBException
     */
    public static void deleteAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            db.delete(iter.key());
        }

        iter.close();
    }
}

