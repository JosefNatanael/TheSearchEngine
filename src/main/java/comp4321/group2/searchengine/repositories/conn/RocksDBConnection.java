package comp4321.group2.searchengine.repositories.conn;

import org.rocksdb.RocksDB;

public class RocksDBConnection {

    private final RocksDB connection;
    private final String dbName;

    public RocksDBConnection(RocksDB connection, String dbName) {
        this.connection = connection;
        this.dbName = dbName;
    }

    public RocksDB getConnection() {
        return connection;
    }

    public String getDbName() {
        return dbName;
    }

    public void closeConnection() {
        connection.close();
    }
}
