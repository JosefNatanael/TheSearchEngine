package comp4321.group2.searchengine.crawler;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.repositories.ForwardIndex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;
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

    public static void main(String[] args) throws RocksDBException {
        RocksDBApi.connect();
        RocksDBApi.reset();
        String rootUrl = "https://www.cse.ust.hk/";
        FastCrawler crawler = new FastCrawler(rootUrl);
        crawler.indexToDB();
    }
}
