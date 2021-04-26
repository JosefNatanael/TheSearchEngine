package comp4321.group2.searchengine.repositories;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetAllEntriesStrategy;
import comp4321.group2.searchengine.repositories.strategies.interfaces.IGetValueStrategy;
import comp4321.group2.searchengine.utils.ByteIntUtilities;

public abstract class RocksDBRepo {

    protected final RocksDB db;

    protected IGetValueStrategy getValueStrategy;
    protected IGetAllEntriesStrategy getAllEntriesStrategy;

    public RocksDBRepo(RocksDB db) {
        this.db = db;
    }

    public Object getValue(byte[] key) throws Exception {
        return getValueStrategy.getValue(key, db);
    }

    public Object getAllEntries() throws Exception {
        return getAllEntriesStrategy.getAllEntries(db);
    }

    public void addEntry(byte[] key, byte[] values) throws RocksDBException {
        db.put(key, values);
    }

    public void delEntry(byte[] key) throws RocksDBException {
        db.delete(key);
    }

    /**
     * Prints all the data in the DB hashtable to the console
     */
    public void printAll() {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + "\t=\t" + ByteIntUtilities.convertByteArrayToInt(iter.value()));
        }

        iter.close();
    }

    /**
     * Deletes all the data in the DB
     */
    public void deleteAll() throws RocksDBException {
        RocksIterator iter = db.newIterator();

        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            db.delete(iter.key());
        }

        iter.close();
    }
}
