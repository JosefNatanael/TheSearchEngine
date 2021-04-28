package comp4321.group2.searchengine.repositories.pool;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.repositories.conn.RocksDBConnection;

/**
 * A blocking pool of RocksDBConnections.
 */
public class BlockingRocksDBConnectionPool {

    // RocksDB Options may have the following configuration. If not specified, will use default values.
    public static final String ROCKSDB_DATA_FOLDER = "rocksdb.dataFolder";
    public static final String ROCKSDB_WRITE_BUFFER = "rocksdb.writeBuffer";
    public static final String ROCKSDB_TARGET_FILE_SIZE = "rocksdb.targetFile";
    public static final String ROCKSDB_UNIVERSAL_COMPACTION = "rocksdb.compaction.universal";
    public static final String ROCKSDB_PREFIX_EXTRACTOR_MODE = "rocksdb.prefixExtractorMode";

    private final Map<String, BlockingQueue<RocksDBConnection>> dbNameToQueueMap;
    private final List<Options> optionsList;
    private final Set<String> dbSet;

    /**
     * dbSet contains dbNames we want to have in our connection pool.
     * All dbNames to be used must be specified during construction (which is now),
     * since there are no mutator methods to add more dbNames.
     *
     * To initalize the dbNames with actual db connections, invoke the init() method once per dbName.
     */
    public BlockingRocksDBConnectionPool(Set<String> dbSet) {
        this.dbSet = dbSet;
        this.dbNameToQueueMap = new HashMap<>();
        this.optionsList = new ArrayList<>();
    }

    /**
     * Get a Connection for the dbName from the connection pool
     * @param dbName Name of the database instance
     * @return A live connection to the database
     */
    public RocksDBConnection getConnection(String dbName) throws Exception {
        return dbNameToQueueMap.get(dbName).take();
    }

    /**
     * Release a previously acquired connection back to the connection pool
     * @param connection Previously acquired connection
     */
    public void releaseConnection(RocksDBConnection connection) throws Exception {
        dbNameToQueueMap.get(connection.getDbName()).put(connection);
    }

    /**
     * Closes all open connections
     */
    public void closeAllOpenConnections() throws Exception {
        for (String currentDb : dbNameToQueueMap.keySet()) {
            BlockingQueue<RocksDBConnection> queue = dbNameToQueueMap.get(currentDb);
            while (!queue.isEmpty()) {
                RocksDBConnection connection = queue.take();
                connection.closeConnection();
            }
        }

        for (Options options : optionsList) {
            options.close();
        }
    }

    /**
     * Initialize a single db connection for dbName using the given properties.
     * @return true if everything goes well, otherwise false.
     */
    public boolean init(String dbName, Properties prop) throws Exception {
        // Get full database absolute path
        File absBasePathFile = new File(Constants.databaseRootDir);
        String absBasePath = absBasePathFile.getAbsolutePath();
        String fullDBPath;
        String temp = prop.getProperty(ROCKSDB_DATA_FOLDER);

        if (temp == null) {
            System.out.println("Directory path not specified");
            return false;
        } else if (temp.endsWith("/")) {
            fullDBPath = absBasePath + "/" + temp.substring(0, temp.length() - 1);
        } else {
            fullDBPath = absBasePath + "/" + temp;
        }

        // Get write buffer size from properties
        long writeBufferSize;
        temp = prop.getProperty(ROCKSDB_WRITE_BUFFER);
        if (temp == null) {
            writeBufferSize = 64 * SizeUnit.MB;
            System.out.println("Write buffer size is set to " + writeBufferSize + " since it is not specified");
        } else {
            writeBufferSize = Long.parseLong(temp);
            System.out.println("Write buffer size: " + writeBufferSize);
        }

        // Get target file size from properties
        int targetFileSize;
        temp = prop.getProperty(ROCKSDB_TARGET_FILE_SIZE);
        if (temp == null) {
            targetFileSize = (int) (64 * SizeUnit.MB);
            System.out.println("Target file size is set to " + targetFileSize + " since it is not specified");
        } else {
            targetFileSize = Integer.parseInt(temp);
            System.out.println("Target file size: " + targetFileSize);
        }

        // Get universal compaction from properties
        boolean isUniversalCompaction;
        temp = prop.getProperty(ROCKSDB_UNIVERSAL_COMPACTION);
        if (temp == null) {
            isUniversalCompaction = false;
        } else {
            temp = temp.toLowerCase();
            isUniversalCompaction =
                temp.equals("1") || temp.equals("true") || temp.equals("yes");
        }
        System.out.println("Universal compaction: " + isUniversalCompaction);

        // Check if use prefix extractor option
        boolean usePrefixExtractor = false;
        temp = prop.getProperty(ROCKSDB_PREFIX_EXTRACTOR_MODE);
        if (temp == "true") {
            usePrefixExtractor = true;
        }

        // Create a RocksDB connection for dbName, and bookkeeping
        if (!dbSet.contains(dbName)) return false;

        Options opt = new Options();
        if (isUniversalCompaction) opt.setCompactionStyle(CompactionStyle.UNIVERSAL);
        if (usePrefixExtractor) opt.useFixedLengthPrefixExtractor(Constants.prefixBytesLength);
        opt.setCreateIfMissing(true);
        opt.setWriteBufferSize(writeBufferSize);
        opt.setTargetFileSizeBase(targetFileSize);
        optionsList.add(opt);

        BlockingQueue<RocksDBConnection> que = new LinkedBlockingQueue<>();
        RocksDBConnection conn = new RocksDBConnection(RocksDB.open(opt, fullDBPath), dbName);
        que.add(conn);
        dbNameToQueueMap.put(dbName, que);

        System.out.println("Opened RocksDB connection to " + fullDBPath);
        return true;
    }
}
