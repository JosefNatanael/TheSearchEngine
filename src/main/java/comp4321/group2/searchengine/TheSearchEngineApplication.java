package comp4321.group2.searchengine;

import comp4321.group2.searchengine.crawler.FastCrawler;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.precompute.FastCompute;
import comp4321.group2.searchengine.repositories.Metadata;
import comp4321.group2.searchengine.repositories.WordIdToIdf;
import comp4321.group2.searchengine.repositories.WordToWordId;
import org.rocksdb.RocksDBException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;

@SpringBootApplication
public class TheSearchEngineApplication extends SpringBootServletInitializer {

    public static void main(String[] args) throws RocksDBException, IOException, ClassNotFoundException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect(true);

        Scanner scanner = new Scanner(System.in);

        System.out.println("\n\n\t\tWelcome to The Search Engine!\n\n");
        System.out.println("To crawl or not to crawl? (c/n)");
        String isIndex_string = scanner.nextLine();
        if (isIndex_string.trim().equalsIgnoreCase("c")) {
            System.out.println("From scratch or not from scratch? (s/n)");
            String fromScratch_string = scanner.nextLine();
            boolean checkLastModified = true;
            int minNumCrawled;

            while (true) {
                System.out.println("Enter minimum number of page you wish to index: (-1 for default)");
                String minNumCrawled_string = scanner.nextLine();

                try {
                    int n = Integer.parseInt(minNumCrawled_string);
                    minNumCrawled = n == -1 ? 4000 : n;
                    break;
                } catch (NumberFormatException e) {
                    System.out.println("Please enter an integer");
                }
            }

            if (fromScratch_string.trim().equalsIgnoreCase("s")) {
                System.out.println("Resetting database...");
                RocksDBApi.reset(true);
                checkLastModified = false;
            }

            String rootUrl = "https://www.cse.ust.hk/";

            FastCrawler crawler = new FastCrawler(rootUrl);
            crawler.indexToDB(checkLastModified, minNumCrawled);

            System.out.println("Precomputing\n...");
            FastCompute compute = new FastCompute();
            compute.processWordIdToIdfEntries();
            System.out.println("......");
            compute.processWeightsAndPageLength();
            System.out.println(".........");
            compute.computePageParents();
            System.out.println("............");
            compute.computePageRank();
            System.out.println("Completed\n");
        }

        System.out.println("Indexed data:");
        Metadata.printAll();

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
