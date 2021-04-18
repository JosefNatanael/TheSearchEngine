package comp4321.group2.searchengine.repositories.conn;

import org.rocksdb.RocksDB;

public class RocksDBConnection {

    private RocksDB connection;
    private String dbName;

    public RocksDBConnection(RocksDB connection, String dbName) throws Exception {
        this.connection = connection;
        this.dbName = dbName;
    }

    public RocksDB getConnection() {
        return connection;
    }

    public String getDbName() {
        return dbName;
    }

    public void closeConnection() throws Exception {
        RocksDB conn = (RocksDB) connection;
        conn.close();
    }
}
