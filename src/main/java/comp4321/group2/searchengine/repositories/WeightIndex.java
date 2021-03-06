package comp4321.group2.searchengine.repositories;

import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class WeightIndex {

    private static RocksDB db;

    public static void connect(boolean isProduction) throws RocksDBException {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.useFixedLengthPrefixExtractor(8);

        // create and open the database
        // create the DB if directory does not exist, then open the DB
        File directory = isProduction ? new File("./src/main/java/tables/WeightIndex") : new File("./src/test/java/tables/WeightIndex");
        String dbPath = directory.getAbsolutePath();
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                System.out.println("Directory created");
            } else {
                System.out.println("Failed to create directory");
            }
        }
        db = RocksDB.open(options, dbPath);
    }

    public static void closeConnection() {
        if (db != null) {
            db.close();
        }
    }

    public static void addEntry(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    //prefix match
    public static void delEntry(String word) throws RocksDBException {
        // Delete the word and its list from the hashtable
        db.delete(word.getBytes());
    }

    // prefix match
    public static HashMap<Integer, Double> getValue(byte[] prefix) {
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(false);
        ro.setPrefixSameAsStart(true);

        HashMap<Integer, Double> wordIdToWeights = new HashMap<>();
        RocksIterator iter = db.newIterator(ro);
        String keyStr;
        double value;

        for (iter.seek(prefix); iter.isValid(); iter.next()) {
            keyStr = new String(iter.key());
            value = ByteIntUtilities.convertByteArrayToDouble(iter.value());
            wordIdToWeights.put(
                Integer.parseInt(WordUtilities.getSuffixFromKeyString(keyStr)),
                value
            );
        }

        iter.close();
        return wordIdToWeights;
    }

    public static double getValueByKey(byte[] key) throws RocksDBException {
        byte[] value = db.get(key);
        if (value == null) return 0.0;
        return ByteIntUtilities.convertByteArrayToDouble(value);
    }

    /**
     * Get all the result pairs
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
     * Print all the data in the DB to the console
     */
    public static void printAll() {
        // Print all the data in the hashtable
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + "\t=\t" + ByteIntUtilities.convertByteArrayToDouble(iter.value()));
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

    /**
     * Creates NEW entries in the database in batch.
     * Table: <invertedIndexKey: byte array, locations: int array>
     */
    public static void createEntriesInBatch(Map<byte[], Double> table)
        throws RocksDBException {
        byte[] keyword;
        double weight;

        for (Entry<byte[], Double> it : table.entrySet()) {
            keyword = it.getKey();
            weight = it.getValue();
            db.put(keyword, ByteIntUtilities.doubleToByteArray(weight));
        }
    }
}

