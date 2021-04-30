package comp4321.group2.searchengine.crawler;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.repositories.Metadata;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class FastCrawlerTest {

    @Test
    void testWholeClass() throws RocksDBException, IOException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect(false);
        RocksDBApi.reset(false);

        String rootUrl = "https://www.cse.ust.hk/";

        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB(false, 1);

        HashMap<String, Integer> metadata = Metadata.getAll();
        assertTrue(metadata.get("page") > 0);
        assertTrue(metadata.get("word") > 0);

        RocksDBApi.closeAllDBConnections();
    }
}
