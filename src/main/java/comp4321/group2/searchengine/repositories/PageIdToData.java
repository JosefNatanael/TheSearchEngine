package comp4321.group2.searchengine.repositories;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.utils.ByteIntUtilities;

public final class PageIdToData {

    private static RocksDB db;
    private static Options options;

    public static void connect() throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        options = new Options();
        options.setCreateIfMissing(true);

        // create the DB if directory does not exist, then open the DB
        File directory = new File("./src/main/java/tables/PageIdToData");
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

    public static byte[] getValue(byte[] key) throws RocksDBException {
        byte[] value = db.get(key);
        return value;
    }

    public static void addEntry(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    public static void delEntry(byte[] key) throws RocksDBException {
        db.delete(key);
    }

    /**
     * Get all the result pairs
     * @throws RocksDBException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static HashMap<Integer, Page> getAll() throws RocksDBException, ClassNotFoundException, IOException {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, Page> result = new HashMap<Integer, Page>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), Page.deserialize(iter.value()));
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
            System.out.println(ByteIntUtilities.convertByteArrayToInt(iter.key()) + "\t=\t" + new String(iter.value()));
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
