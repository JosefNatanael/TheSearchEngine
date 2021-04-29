package comp4321.group2.searchengine.repositories;

import java.io.File;
import java.util.HashMap;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import comp4321.group2.searchengine.utils.ByteIntUtilities;

public final class WordIdToWord {

    private static RocksDB db;

    public static void connect(boolean isProduction) throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();
        options.setCreateIfMissing(true);

        // create the DB if directory does not exist, then open the DB
        File directory = isProduction ? new File("./src/main/java/tables/WordIdToWord") : new File("./src/test/java/tables/WordIdToWord");
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

    public static String getValue(int key) throws RocksDBException {
        byte[] value = db.get(ByteIntUtilities.convertIntToByteArray(key));
        return new String(value);
    }

    public static void addEntry(int key, String value) throws RocksDBException {
        db.put(ByteIntUtilities.convertIntToByteArray(key), value.getBytes());
    }

    public static void delEntry(int key) throws RocksDBException {
        db.delete(ByteIntUtilities.convertIntToByteArray(key));
    }

    /**
     * Get all the result pairs
     *
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
     * Prints all the data in the DB hashtable to the console
     *
     */
    public static void printAll() {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(ByteIntUtilities.convertByteArrayToInt(iter.key())  + "\t=\t" + new String(iter.value()));
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
