package comp4321.group2.searchengine.crawler;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.precompute.FastCompute;
import comp4321.group2.searchengine.query.QueryHandler;
import comp4321.group2.searchengine.repositories.Metadata;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.*;


public class FastCrawler {
    private final String startingUrl;

    public FastCrawler(String startingUrl) {
        this.startingUrl = startingUrl;
    }

    public void indexToDB(boolean checkLastModified, int maxNumIndex) {

        BlockingQueue<Link> urlQueue = new LinkedBlockingQueue<>(); // the queue of URLs to be crawled
        Set<String> urls = ConcurrentHashMap.newKeySet(); // the set of urls that have been visited before
        urlQueue.add(new Link(startingUrl, 1));

        ExecutorService executor = Executors.newFixedThreadPool(Constants.numCrawlerThreads);
        CountDownLatch latch = new CountDownLatch(Constants.numCrawlerThreads);   // When zero, interrupt all threads (they might be deadlocked)

        System.out.println("Starting fast crawler");
        long start = System.currentTimeMillis();

        ArrayList<ImmutablePair<Future, CrawlerRunnable>> spawnedThreads = new ArrayList<>();
        for (int i = 0; i < Constants.numCrawlerThreads; ++i) {
            CrawlerRunnable r = new CrawlerRunnable(urlQueue, urls, latch, checkLastModified, maxNumIndex);
            Future<?> f = executor.submit(r);
            ImmutablePair<Future, CrawlerRunnable> pair = new ImmutablePair<>(f, r);
            spawnedThreads.add(pair);
        }
        executor.shutdown();

        Thread threadKiller = new Thread() {
            public void run(){
                Scanner sc = new Scanner(System.in);
//                System.out.println("Return to stop indexing");
                sc.nextLine();
//                System.out.println("Notifying all crawler threads to stop now...");
//
//                Set<Thread> threads = Thread.getAllStackTraces().keySet();
//
//                for (Thread t : threads) {
//                    String name = t.getName();
//                    Thread.State state = t.getState();
//                    int priority = t.getPriority();
//                    String type = t.isDaemon() ? "Daemon" : "Normal";
//                    System.out.printf("%-20s \t %s \t %d \t %s\n", name, state, priority, type);
//                }

                // Notifies all running threads to stop scraping
                spawnedThreads.forEach((pair) -> pair.getValue().setStopScraping(true));
            }
        };

        threadKiller.start();

        try {
            latch.await();
            threadKiller.interrupt();
            spawnedThreads.forEach((pair) -> pair.getKey().cancel(true));
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        long end = System.currentTimeMillis();

        System.out.println("Total time taken: " + (end - start));
        System.out.println("Number of unique pages seen (indexed or not): " + urls.size());
        System.out.println(urlQueue.size());
        System.out.println("Done");
    }


    public static void main(String[] args) throws RocksDBException, InvalidWordIdConversionException, IOException, ClassNotFoundException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect();

        Scanner scanner = new Scanner(System.in);

        System.out.println("\n\n\t\tWelcome to The Search Engine!\n\n");
        System.out.println("Do you want to crawl and index a new database? (y/n)");
        String isIndex_string = scanner.nextLine();
        if(isIndex_string.trim().toLowerCase().equals("y")){
            System.out.println("Do you want to start from scratch? (y/n)");
            String fromScratch_string = scanner.nextLine();
            boolean checkLastModified =  true;
            int minNumCrawled;

            while(true){
                System.out.println("Enter minimum number of page you wish to index: (-1 for default)");
                String minNumCrawled_string = scanner.nextLine();

                try{
                    int n = Integer.parseInt(minNumCrawled_string);
                    minNumCrawled = n == -1 ? 4000 : n;
                    break;
                } catch (NumberFormatException e){
                    System.out.println("Please enter an integer");
                }
            }

            if(fromScratch_string.trim().toLowerCase().equals("y")){
                System.out.println("Resetting database...");
                RocksDBApi.reset();
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
            System.out.println("Completed\n");
        }

        System.out.println("Indexed data:");
        Metadata.printAll();

//        PageIdToParentIds.printAll();
//        URLToPageId.printAll();
//        WeightIndex.printAll();
//        WordIdToIdf.printAll();
//        PageIdToLength.printAll();

        System.out.println("Press Return to start the backend service...");
        //baru function start spring applications
        // SpringApplication.run(TheSearchEngineApplication.class, args);

        //debugging purposes
        while (true) {
            System.out.println("\nEnter your query (enter :q to quit) :");
//            scanner.nextLine();

            String query = scanner.nextLine();
            if (query.equals(":q")) break;

            QueryHandler qh = new QueryHandler(query);
            qh.handle();
        }
        System.out.println("\n\n\t\t\t  Thank You!\n\tWe hope you enjoy our services!\n\n");
        RocksDBApi.closeAllDBConnections();
    }
}
