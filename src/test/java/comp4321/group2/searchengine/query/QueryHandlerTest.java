package comp4321.group2.searchengine.query;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.crawler.FastCrawler;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.precompute.FastCompute;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class QueryHandlerTest {

    @Test
    void testWholeClass() throws RocksDBException, IOException, ClassNotFoundException, InvalidWordIdConversionException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect(false);

        FastCrawler crawler = new FastCrawler("https://www.cse.ust.hk");
        crawler.indexToDB(false, 1);

        FastCompute fastCompute = new FastCompute();
        fastCompute.processWordIdToIdfEntries();
        fastCompute.processWeightsAndPageLength();
        fastCompute.computePageParents();
        fastCompute.computePageRank();

        QueryHandler qh = new QueryHandler("undergraduate");
        assertTrue(qh.handle().size() > 1);

        RocksDBApi.closeAllDBConnections();
    }
}
