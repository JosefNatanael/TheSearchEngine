package comp4321.group2.searchengine.repositories;

import comp4321.group2.searchengine.utils.ByteIntUtilities;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.HashMap;

public final class Metadata {

    private static RocksDB db;

    //enumerate
    public enum Key {
        PAGE,
        WORD,
    }

    public static void connect(boolean isProduction) throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();
        options.setCreateIfMissing(true);

        // create the DB if directory does not exist, then open the DB
        File directory = isProduction ? new File("./src/main/java/tables/Metadata") : new File("./src/test/java/tables/Metadata");
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

    public synchronized static int getLatestIndex(Key inputKey) throws RocksDBException {
        String key = inputKey == Key.PAGE ? "page" : "word";
        byte[] keyBytes = key.getBytes();
        byte[] content = db.get(keyBytes);
        int index = content == null ? 0 : ByteIntUtilities.convertByteArrayToInt(content) + 1;

        content = ByteIntUtilities.convertIntToByteArray(index);
        db.put(keyBytes, content);
        return index;
    }

    public static void delEntry(String word) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(word.getBytes());
    }

    /**
     * Get all the result pairs
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
     */
    public static void deleteAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            db.delete(iter.key());
        }

        iter.close();
    }
}
