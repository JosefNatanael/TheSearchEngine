package comp4321.group2.searchengine;

import comp4321.group2.searchengine.crawler.FastCrawler;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.repositories.*;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.RocksDBException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@SpringBootApplication
public class TheSearchEngineApplication extends SpringBootServletInitializer {

    public static void main(String[] args) throws RocksDBException {
//        RocksDBApi.closeAllDBConnections();
//        RocksDBApi.connect();
//        RocksDBApi.reset();
//        phaseOne();
//        RocksDBApi.closeAllDBConnections();


        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect();
        RocksDBApi.reset();
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB();
        Metadata.printAll();


        SpringApplication.run(TheSearchEngineApplication.class, args);
    }

    public static void startIndexer() {
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB();
    }

    public static void phaseOne()
            throws RocksDBException, InvalidWordIdConversionException, ClassNotFoundException, IOException {
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB();

        PrintWriter writer = new PrintWriter("spider_result.txt");

        HashMap<String, HashMap<String, ArrayList<Integer>>> godMap = new HashMap<>();
        HashMap<String, Integer> wordToWordIdMap = WordToWordId.getAll();
        for (Map.Entry<String, Integer> pair : wordToWordIdMap.entrySet()) {
            String word = pair.getKey();
            int wordId = pair.getValue();

            byte[] byteKey = WordUtilities.buildDBKeyPrefix(wordId).toString().getBytes();
            HashMap<String, ArrayList<Integer>> pageIdToWordLocsMap = InvertedIndex.getValue(byteKey);
            godMap.put(word, pageIdToWordLocsMap);
        }

        HashMap<String, Integer> urlToPageIdMap = URLToPageId.getAll();
        for (Map.Entry<String, Integer> pair : urlToPageIdMap.entrySet()) {
            String url = pair.getKey();
            int pageId = pair.getValue();
            Page pageData = RocksDBApi.getPageData(url);

            DateTimeFormatter dateformat = DateTimeFormatter.RFC_1123_DATE_TIME;
            ZonedDateTime lastMod = pageData.getLastModified();
            String dateTime;

            if (lastMod != null) {
                dateTime = lastMod.format(dateformat);
            } else {
                dateTime = "n/a";
            }

            writer.println("Page title: " + pageData.getTitle());
            writer.println("URL: " + url);
            writer.println("Last modification date: " + dateTime + ", size of page: " + pageData.getSize() + " Bytes");
            writer.println("\nKeywords and frequency: ");
            for (Map.Entry<String, HashMap<String, ArrayList<Integer>>> innerPair : godMap.entrySet()) {
                HashMap<String, ArrayList<Integer>> pageIdToWordLocsMap = innerPair.getValue();
                if (pageIdToWordLocsMap.containsKey(Integer.toString(pageId))) {
                    writer.print(
                            innerPair.getKey() + " " + pageIdToWordLocsMap.get(Integer.toString(pageId)).size() + "; "
                    );
                }
            }

            writer.println("\nChild links: ");
            String[] childLinksArray = pageData.getChildUrls().split("\t");
            for (String s : childLinksArray) {
                if (s.equals("")) {
                    continue;
                }
                if (s.charAt(0) == '/') {
                    writer.println(rootUrl + s.substring(1));
                } else {
                    writer.println(s);
                }
            }

            writer.println("------------------------------------------------------------\n");
        }
        writer.close();
    }

    public void completed()
        throws RocksDBException, InvalidWordIdConversionException {
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB();

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
