package comp4321.group2.searchengine.precompute;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.crawler.FastCrawler;
import comp4321.group2.searchengine.repositories.PageIdToLength;
import comp4321.group2.searchengine.repositories.PageIdToParentIds;
import comp4321.group2.searchengine.repositories.WeightIndex;
import comp4321.group2.searchengine.repositories.WordIdToIdf;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FastComputeTest {

    @Test
    void testWholeClass() throws IOException, ClassNotFoundException, RocksDBException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect(false);

        FastCrawler crawler = new FastCrawler("https://www.cse.ust.hk");
        crawler.indexToDB(false, 1);

        FastCompute fastCompute = new FastCompute();
        fastCompute.processWordIdToIdfEntries();
        fastCompute.processWeightsAndPageLength();
        fastCompute.computePageParents();

        assertTrue(WordIdToIdf.getAll().size() > 0);
        assertTrue(WeightIndex.getAll().size() > 0);
        assertTrue(PageIdToLength.getAll().size() > 0);
        assertTrue(PageIdToParentIds.getAll().size() > 0);

        RocksDBApi.closeAllDBConnections();
    }

}
