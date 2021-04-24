package comp4321.group2.searchengine.repositories;

import comp4321.group2.searchengine.utils.ByteIntUtilities;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.HashMap;

public class PageIdToLength {

    private static RocksDB db;
    private static Options options;

    public static void connect() throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        options = new Options();
        options.setCreateIfMissing(true);

        // create the DB if directory does not exist, then open the DB
        File directory = new File("./src/main/java/tables/PageIdToLength");
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

    public static double getValue(int key) throws RocksDBException {
        byte[] value = db.get(ByteIntUtilities.convertIntToByteArray(key));
        double result = value != null ? ByteIntUtilities.convertByteArrayToDouble(value) : -1.0;
        return result;
    }

    public static void addEntry(int key, double value) throws RocksDBException {
        db.put(ByteIntUtilities.convertIntToByteArray(key), ByteIntUtilities.doubleToByteArray(value));
    }

    public static void delEntry(int key) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(ByteIntUtilities.convertIntToByteArray(key));
    }

    /**
     * Get all the result pairs
     * @throws RocksDBException
     */
    public static HashMap<Integer, Double> getAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();
        HashMap<Integer, Double> result = new HashMap<Integer, Double>();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), ByteIntUtilities.convertByteArrayToDouble(iter.value()));
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
            System.out.println(ByteIntUtilities.convertByteArrayToInt(iter.key()) + "\t=\t" + ByteIntUtilities.convertByteArrayToDouble(iter.value()));
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


