package comp4321.group2.searchengine.repositories;

import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class PageIdToParentId {

//    private static RocksDB db;
//
//    public static void connect() throws RocksDBException {
//        // the Options class contains a set of configurable DB options
//        // that determines the behaviour of the database.
//        Options options = new Options();
//        options.setCreateIfMissing(true);
//
//        // create the DB if directory does not exist, then open the DB
//        File directory = new File("./src/main/java/tables/PageIdToParentId");
//        String dbPath = directory.getAbsolutePath();
//        if (!directory.exists()) {
//            directory.mkdir();
//        }
//        db = RocksDB.open(options, dbPath);
//    }
//
//    public static void closeConnection() {
//        if (db != null) {
//            db.close();
//        }
//    }
//
//    public static ArrayList<Integer> getValue(int key) throws RocksDBException, IOException, ClassNotFoundException {
//        String value = new String(db.get(ByteIntUtilities.convertIntToByteArray(key)));
//        return WordUtilities.stringToIntegerArrayList(value);
//    }
//
//    public static void addEntry(int key, List<Integer> parentIds) throws RocksDBException, IOException {
////        db.put(ByteIntUtilities.convertIntToByteArray(key), Page.serialize(value));
//    }
//
//    public static void delEntry(int key) throws RocksDBException {
//        db.delete(ByteIntUtilities.convertIntToByteArray(key));
//    }
//
//    /**
//     * Get all the result pairs
//     *
//     */
//    public static HashMap<Integer, String> getAll() throws ClassNotFoundException, IOException {
////        RocksIterator iter = db.newIterator();
////        HashMap<Integer, Integer> result = new HashMap<>();
////
////        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
////            result.put(ByteIntUtilities.convertByteArrayToInt(iter.key()), Page.deserialize(iter.value()));
////        }
////
////        iter.close();
////        return result;
//    }
//
//    /**
//     * Prints all the data in the DB hashtable to the console
//     *
//     */
//    public static void printAll() {
////        RocksIterator iter = db.newIterator();
////
////        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
////            System.out.println(ByteIntUtilities.convertByteArrayToInt(iter.key()) + "\t=\t" + new String(iter.value()));
////        }
////
////        iter.close();
//    }
//
//    /**
//     * Delete all the data in the DB
//     *
//     */
//    public static void deleteAll() throws RocksDBException {
//        RocksIterator iter = db.newIterator();
//
//        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
//            db.delete(iter.key());
//        }
//
//        iter.close();
//    }
}
