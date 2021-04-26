package comp4321.group2.searchengine.repositories;

import java.io.File;
import java.util.HashMap;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import comp4321.group2.searchengine.utils.ByteIntUtilities;

public final class URLToPageId {

    private static RocksDB db;

    public static void connect() throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();
        options.setCreateIfMissing(true);

        // create the DB if directory does not exist, then open the DB
        File directory = new File("./src/main/java/tables/URLToPageId");
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

    public static int getValue(String key) throws RocksDBException {
        byte[] value = db.get(key.getBytes());
        return value != null ? ByteIntUtilities.convertByteArrayToInt(value) : -1;
    }

    public static void addEntry(String key, int value) throws RocksDBException {
        db.put(key.getBytes(), ByteIntUtilities.convertIntToByteArray(value));
    }

    public static void delEntry(String key) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(key.getBytes());
    }

    /**
     * Get all the result pairs
     *
     */
    public static HashMap<String, Integer> getAll() {
        RocksIterator iter = db.newIterator();
        HashMap<String, Integer> result = new HashMap<>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(new String(iter.key()), ByteIntUtilities.convertByteArrayToInt(iter.value()));
        }

        iter.close();
        return result;
    }

    /**
     * Prints all the data in the DB hashtable to the console
     *
     */
    public static void printAll() {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + "\t=\t" + ByteIntUtilities.convertByteArrayToInt(iter.value()));
        }

        iter.close();
    }

    /**
     * Delete all the data in the DB
     *
     */
    public static void deleteAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            db.delete(iter.key());
        }

        iter.close();
    }
}
