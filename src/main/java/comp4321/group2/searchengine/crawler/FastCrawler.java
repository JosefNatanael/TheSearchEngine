package comp4321.group2.searchengine.crawler;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.repositories.ForwardIndex;
import comp4321.group2.searchengine.repositories.WordIdToIdf;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.*;


public class FastCrawler {
    private String startingUrl;

    public FastCrawler(String startingUrl) throws RocksDBException {
        this.startingUrl = startingUrl;
    }

    public void indexToDB() {

        BlockingQueue<Link> urlQueue = new LinkedBlockingQueue<Link>(); // the queue of URLs to be crawled
        Set<String> urls = ConcurrentHashMap.newKeySet(); // the set of urls that have been visited before
        urlQueue.add(new Link(startingUrl, 1));

        ExecutorService executor = Executors.newFixedThreadPool(Constants.numCrawlerThreads);
        CountDownLatch latch = new CountDownLatch(Constants.numCrawlerThreads);   // When zero, interrupt all threads (they might be deadlocked)

        System.out.println("Starting fast crawler");
        long start = System.currentTimeMillis();

        ArrayList<ImmutablePair<Future, CrawlerRunnable>> spawnedThreads = new ArrayList<>();
        for (int i = 0; i < Constants.numCrawlerThreads; ++i) {
            CrawlerRunnable r = new CrawlerRunnable(urlQueue, urls, latch);
            Future f = executor.submit(r);
            ImmutablePair<Future, CrawlerRunnable> pair = new ImmutablePair<>(f, r);
            spawnedThreads.add(pair);
        }
        executor.shutdown();

        Scanner sc = new Scanner(System.in);
        System.out.println("Return to stop indexing");
        sc.nextLine();
        System.out.println("Notifying all crawler threads to stop now...");

        // Notifies all running threads to stop scraping
        spawnedThreads.forEach((pair) -> {
            pair.getValue().setStopScraping(true);
        });

//        // Force to stop all threads, running or not
//        spawnedThreads.forEach((pair) -> {
//            pair.getKey().cancel(true);
//        });

        try {
            latch.await();
            spawnedThreads.forEach((pair) -> {
                pair.getKey().cancel(true);
            });
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        long end = System.currentTimeMillis();

        System.out.println("Total time taken: " + (end - start));
        System.out.println("Number of unique pages seen (indexed): " + urls.size());
    }

    public void postIndexProcess() throws RocksDBException, InvalidWordIdConversionException {
        //iterate each word ID, compute idf, length
        HashMap<String, Integer> wordToWordID = RocksDBApi.getAllWordToWordID();
        HashMap<String, Integer> latestIndex = RocksDBApi.getAllMetadata();
        int numDocs = latestIndex.get("page");
        int df;
        double idf;
//        ArrayList<Integer> concatenated = new ArrayList<Integer>();

        for (Map.Entry<String, Integer> pair : wordToWordID.entrySet()) {
            String word = pair.getKey();
            int wordId = pair.getValue();
            ArrayList<Integer> result = RocksDBApi.getPageIdsOfWord(word);
            df = result.size();
            idf = (Math.log(numDocs/(double)df) / Math.log(2));
            //WordIdToIdf.addEntry(idf)

            RocksDBApi.addWordIdf(wordId, idf);

//            concatenated.addAll(result);
        }

//        Set<Integer> pageIds = new HashSet<Integer>(concatenated);
        computeL2Length();
    }

    public void computeL2Length() throws RocksDBException, InvalidWordIdConversionException {
        HashMap<String, Integer> URLToPageID = RocksDBApi.getAllURLToPageID();
        int wordId, tf;
        double idf, accumulator = 0.0, length_result;
        //from forward get k2 length
        for (Map.Entry<String, Integer> pair : URLToPageID.entrySet()) {
            int pageId = pair.getValue();

            //get wordIdList from forward index
            ArrayList<Integer> wordIdList = RocksDBApi.getWordIdListFromPageId(pageId);

            //for each word id
            for(int i = 0; i < wordIdList.size(); ++i){
                wordId = wordIdList.get(i);
                //get idf of the word
                idf = RocksDBApi.getIdfFromWordId(wordId);

                //get tf of the word from getvaluebykey of index 'wordID@pageID'
                tf = RocksDBApi.getInvertedValuesFromKey(wordId, pageId).size();

                //get the square of tf x idf, then accumulate
                accumulator += Math.pow(tf*idf, 2);
            }

            //square root the accumulated squared tf idf
            length_result = Math.sqrt(accumulator);

            //store in db
            RocksDBApi.addPageLength(pageId, length_result);
        }
    }

    public static void main(String[] args) throws RocksDBException, InvalidWordIdConversionException {
        RocksDBApi.connect();
        RocksDBApi.reset();
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB();
        crawler.postIndexProcess();
    }
}
