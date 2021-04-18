package comp4321.group2.searchengine.repositories;

import java.io.File;
import java.util.HashMap;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.utils.ByteIntUtilities;

public final class Metadata {

    private static RocksDB db;
    private static Options options;

    //enumerate
    public static enum Key {
        PAGE,
        WORD,
    }

    public static void connect() throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        options = new Options();
        options.setCreateIfMissing(true);

        // create the DB if directory does not exist, then open the DB
        File directory = new File("./src/main/java/tables/MetaData");
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

    public static int getLatestIndex(Key inputKey) throws RocksDBException {
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
     * @throws RocksDBException
     */
    public static HashMap<String, Integer> getAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();
        HashMap<String, Integer> result = new HashMap<String, Integer>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(new String(iter.key()), ByteIntUtilities.convertByteArrayToInt(iter.value()));
        }

        iter.close();
        return result;
    }

    /**
     * Prints all the data in the DB hashtable to the console
     * @throws RocksDBException
     */
    public static void printAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + "\t=\t" + ByteIntUtilities.convertByteArrayToInt(iter.value()));
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
}
