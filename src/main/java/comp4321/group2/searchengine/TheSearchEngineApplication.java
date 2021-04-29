package comp4321.group2.searchengine;

import comp4321.group2.searchengine.crawler.FastCrawler;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.repositories.Metadata;
import comp4321.group2.searchengine.repositories.WordIdToIdf;
import comp4321.group2.searchengine.repositories.WordToWordId;
import org.rocksdb.RocksDBException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@SpringBootApplication
public class TheSearchEngineApplication extends SpringBootServletInitializer {

    public static void main(String[] args) throws RocksDBException {
//        RocksDBApi.closeAllDBConnections();
//        RocksDBApi.connect();
//        RocksDBApi.reset();
//        phaseOne();
//        RocksDBApi.closeAllDBConnections();


//        RocksDBApi.closeAllDBConnections();
//        RocksDBApi.connect();
//        RocksDBApi.reset();
//        String rootUrl = "https://www.cse.ust.hk/";
//        FastCrawler crawler = new FastCrawler(rootUrl);
//        crawler.indexToDB(false);
//        Metadata.printAll();


        SpringApplication.run(TheSearchEngineApplication.class, args);
    }

    @Bean
    public RestTemplate getRestTemplate() {
        return new RestTemplate();
    }

    public static void startIndexer() {
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB(false, 100);
    }

    public void completed()
        throws RocksDBException, InvalidWordIdConversionException {
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB(false, 100);

        //iterate each word ID, compute idf, length
        HashMap<String, Integer> wordToWordID = WordToWordId.getAll();
        HashMap<String, Integer> latestIndex = Metadata.getAll();
        int numDocs = latestIndex.get("page");
        int df;
        double idf;
        ArrayList<Integer> concatenated = new ArrayList<>();

        for (Map.Entry<String, Integer> pair : wordToWordID.entrySet()) {
            String word = pair.getKey();
            int wordId = pair.getValue();
            ArrayList<Integer> result = RocksDBApi.getPageIdsOfWord(word);
            df = result.size();
            idf = (Math.log(numDocs/(double)df) / Math.log(2));

            WordIdToIdf.addEntry(wordId, idf);

            concatenated.addAll(result);
        }

        Set<Integer> pageIds = new HashSet<>(concatenated);

        //from forward get k2 length
    }

}
