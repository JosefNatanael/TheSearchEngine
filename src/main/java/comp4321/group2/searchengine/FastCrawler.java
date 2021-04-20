package comp4321.group2.searchengine;

import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.utils.StopStem;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
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

        ExecutorService pExecutor = Executors.newFixedThreadPool(4);
        ExecutorService cExecutor = Executors.newFixedThreadPool(4);

        CountDownLatch pLatch = new CountDownLatch(1);
        CountDownLatch cLatch = new CountDownLatch(1);

        System.out.println("Starting fast crawler");
        long start = System.currentTimeMillis();

        pExecutor.submit(new Producer(urlQueue, urls, pLatch));
        cExecutor.submit(new Consumer(urlQueue, urls, cLatch));
        pExecutor.shutdown();
        cExecutor.shutdown();

        try {
            pLatch.await();
            cLatch.await();
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        long end = System.currentTimeMillis();

        System.out.println("Total time taken: " + (end - start));
    }

    public static void main(String args[]) {
    }
}

abstract class CrawlerHelper {
    private static File stopwordsPath = new File("./src/main/resources/stopwords.txt");
    private static StopStem stopStem = new StopStem(stopwordsPath.getAbsolutePath());

    /**
     * Send an HTTP request and analyze the response.
     * @param url
     * @param visitedUrls
     * @return Response res
     * @throws HttpStatusException
     * @throws IOException
     */
    public static Connection.Response getResponse(String url, Set<String> visitedUrls) throws HttpStatusException, IOException {
        if (visitedUrls.contains(url)) {
            throw new RevisitException(); // if the page has been visited, break the function
        }

        Connection conn = Jsoup.connect(url).followRedirects(false);
        // the default body size is 2Mb, to attain unlimited page, use the following
        // Connection conn = Jsoup.connect(this.url).maxBodySize(0).followRedirects(false);
        Connection.Response res;
        try {
            /* establish the connection and retrieve the response */
            res = conn.execute();
            /* if the link redirects to other place... */
            if (res.hasHeader("location")) {
                String actual_url = res.header("location");
                if (visitedUrls.contains(actual_url)) {
                    throw new RevisitException();
                } else {
                    visitedUrls.add(actual_url);
                }
            } else {
                visitedUrls.add(url);
            }
        } catch (HttpStatusException e) {
            throw e;
        }
        return res;
    }

    /** Extract words in the web page content.
     * note: use StringTokenizer to tokenize the result
     * @return Vector<String> a list of words in the web page body
     */
    public static Vector<String> extractWords(Document doc) {
        Vector<String> result = new Vector<String>();
        if (doc == null || doc.body() == null || doc.body().text() == null) {
            return result;
        }
        String contents = doc.body().text();
        StringTokenizer st = new StringTokenizer(contents);
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;
    }

    /** Extract useful external urls on the web page.
     * note: filter out images, emails, etc.
     * @param doc
     * @return Vector<String> a list of external links on the web page
     */
    public static Vector<String> extractLinks(Document doc) {
        Vector<String> result = new Vector<String>();
        Elements links = doc.select("a[href]");
        for (Element link : links) {
            String linkString = link.attr("href");
            // filter out emails
            if (linkString.contains("mailto:")) {
                continue;
            }
            String linkStr = link.attr("href");
            if (linkStr.isEmpty() || linkStr.startsWith("#") || linkStr.startsWith("javascript")) {
                continue;
            }
            if (linkStr.charAt(0) == '/') {
                result.add(doc.location() + linkStr.substring(1));
            } else {
                result.add(link.attr("href"));
            }
        }
        return result;
    }

    /** Use a queue to manage crawl tasks.
     * @throws RocksDBException
     * @throws InvalidWordIdConversionException
     */
//    public static void indexToDB() throws RocksDBException, InvalidWordIdConversionException {
//        int iterationNumber = 0;
//        int numberOfPagesToScrape = Constants.numberOfPagesToScrape;
//
//        while (
//            !this.urlQueue.isEmpty() && Constants.limitNumberOfPagesToScrape && iterationNumber < numberOfPagesToScrape
//        ) {
//            System.out.println("Iteration number: " + Integer.toString(iterationNumber++));
//            Link currentLink = this.urlQueue.remove(0);
//            if (currentLink.level > this.crawlMaxDepth) break; // stop criteria
//            if (this.urls.contains(currentLink.url)) continue; // ignore pages that has been visited
//
//            /* start to crawl on the page */
//            try {
//                Connection.Response res = this.getResponse(currentLink.url);
//                int size = res.bodyAsBytes().length;
//                String lastModified = res.header("last-modified");
//                Document currentDoc = res.parse();
//                Vector<String> links = this.extractLinks(currentDoc);
//
//                extractAndPushChildLinksFromParentToUrlQueue(currentLink, links);
//                HashMap<String, ArrayList<Integer>> wordToWordLocations = extractCleanedWordLocationsMapFromDocument(
//                    currentDoc
//                );
//
//                int tfmax = getTfmax(wordToWordLocations);
//                Page pageData = extractPageData(size, lastModified, currentDoc, links, tfmax);
//                int pageId = RocksDBApi.addPageData(pageData, currentLink.url);
//                RocksDBApi.addPageWords(wordToWordLocations, pageId);
//            } catch (HttpStatusException e) {
//                e.printStackTrace();
//                System.out.printf("\nLink Error: %s\n", currentLink.url);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (RevisitException e) {}
//        }
//    }

    /**
     *
     * @param wordToWordLocations
     * @return tfmax
     */
    public static int getTfmax(HashMap<String, ArrayList<Integer>> wordToWordLocations){
        int tfmax = 0;
        int tf=0;

        for (Map.Entry<String, ArrayList<Integer>> pair : wordToWordLocations.entrySet()) {
            ArrayList<Integer> locations = pair.getValue();
            tf = locations.size();
            if(tf > tfmax) tfmax = tf;
        }
        return tfmax;
    }

    /**
     * Extract child links from current (parent) page link
     * @param parentLink Parent Link
     * @param links Child Links
     * @throws HttpStatusException
     * @throws IOException
     */
    public static void extractAndPushChildLinksFromParentToUrlQueue(Link parentLink, Vector<String> links, BlockingQueue<Link> urlQueue)
        throws HttpStatusException, IOException {
        // Add child links to urlQueue vector
        for (String link : links) {
            urlQueue.add(new Link(link, parentLink.level + 1)); // add links to urlQueue vector
        }
    }

    public static Page extractPageData(int size, String lastModified, Document doc, Vector<String> links, int tfmax) throws IOException {
        /* Get the metadata from the result */
        if (lastModified == null) {
            lastModified = "n/a";
        }

        String title = doc.title();
        String urls = WordUtilities.arrayListToString(new ArrayList<String>(links));
        Page pageData = new Page(title, urls, size, lastModified, tfmax);
        return pageData;
    }

    /**
     * Clean: stopwords removed, stemmed with porter
     * Extracts the word locations for each word in current document page
     * @return HashMap<String, ArrayList<Integer>> key: word string, value: array list of integer locations
     */
    public static HashMap<String, ArrayList<Integer>> extractCleanedWordLocationsMapFromDocument(Document document) {
        Vector<String> words = extractWords(document);
        HashMap<String, ArrayList<Integer>> wordToWordLocationMap = new HashMap<String, ArrayList<Integer>>();
        for (int i = 0; i < words.size(); ++i) {
            String currWord = words.get(i);
            String stemmedWord = stopStem.stem(currWord);
            if (stopStem.isStopWord(currWord) || stemmedWord.equals("")) {
                continue;
            }
            if (wordToWordLocationMap.containsKey(stemmedWord)) {
                wordToWordLocationMap.get(stemmedWord).add(i);
            } else {
                ArrayList<Integer> arrList = new ArrayList<Integer>();
                arrList.add(i);
                wordToWordLocationMap.put(stemmedWord, arrList);
            }
        }
        for (Map.Entry<String, ArrayList<Integer>> pair : wordToWordLocationMap.entrySet()) {
            ArrayList<Integer> locations = pair.getValue();
            Collections.sort(locations);
            wordToWordLocationMap.put(pair.getKey(), locations);
        }

        return wordToWordLocationMap;
    }
}

class Producer implements Runnable {

    private BlockingQueue<Link> urlQueue;
    private Set<String> urls;
    private CountDownLatch pLatch;

    public Producer(BlockingQueue<Link> urlQueue, Set<String> urls, CountDownLatch pLatch) {
        this.urlQueue = urlQueue;
        this.urls = urls;
        this.pLatch = pLatch;
    }

    @Override
    public void run() {
        System.out.println("Producer started");
        if (!urlQueue.isEmpty()) {
            pLatch.countDown();
            return;
        }
        try {
            Link currentLink = urlQueue.take();
            if (currentLink.level > Constants.crawlMaxDepth) {
                pLatch.countDown();
                return;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        pLatch.countDown();
    }
}

class Consumer implements Runnable {

    private BlockingQueue<Link> urlQueue;
    private Set<String> urls;
    private CountDownLatch cLatch;

    public Consumer(BlockingQueue<Link> urlQueue, Set<String> urls, CountDownLatch cLatch) {
        this.urlQueue = urlQueue;
        this.urls = urls;
        this.cLatch = cLatch;
    }

    @Override
    public void run() {
        while (true) {
            try {
//                int num = urlQueue.take();
//                System.out.println("Consumed: " + num);
            } catch (Exception err) {
                err.printStackTrace();
            }
        }
//        cLatch.countDown();
    }
}
