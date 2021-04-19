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

    public static void addEntry(byte[] key, byte[] values) throws RocksDBException {
        db.put(key, values);
    }

    //prefix match
    public static void delEntry(String word) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(word.getBytes());
    }

    public static HashMap<Integer, Integer> getValue(int key) throws RocksDBException {
        byte[] value = db.get(ByteIntUtilities.convertIntToByteArray(key));

        return null;
    }

    /**
     * Get all the result pairs
     * @throws RocksDBException
     */
    public static HashMap<Integer, String> getAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, String> result = new HashMap<Integer, String>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), new String(iter.value()));
        }

        iter.close();
        return result;
    }

    /**
     * Print all the data in the DB to the console
     * @throws RocksDBException
     */
    public static void printAll() throws RocksDBException {
        // Print all the data in the hashtable
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + "\t=\t" + new String(iter.value()));
        }

        iter.close();
    }

    /**
     * Delete all the data in the DB
     * @throws RocksDBException
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
     * @return
     */
    public static void createEntriesInBatch(Map<byte[], ArrayList<Integer>> table, int documentId)
        throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        WriteOptions writeOptions = new WriteOptions();
        byte[] keyword;
        ArrayList<Integer> locations;

        for (Entry<byte[], ArrayList<Integer>> it : table.entrySet()) {
            keyword = it.getKey();
            locations = it.getValue();

            // Step 2: Put into write batch
            writeBatch.put(keyword, WordUtilities.arrayListToString(locations).getBytes());
        }

        db.write(writeOptions, writeBatch);
    }
}

