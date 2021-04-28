package comp4321.group2.searchengine.crawler;


import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.repositories.ForwardIndex;
import comp4321.group2.searchengine.repositories.PageIdToData;
import comp4321.group2.searchengine.repositories.URLToPageId;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class CrawlerRunnable implements Runnable {
    private final BlockingQueue<Link> urlQueue;
    private final Set<String> urls;
    private final CountDownLatch latch;
    private final boolean checkLastModified;
    private volatile boolean stopScraping = false;

    public CrawlerRunnable(BlockingQueue<Link> urlQueue, Set<String> urls, CountDownLatch latch, boolean checkLastModified) {
        this.urlQueue = urlQueue;
        this.urls = urls;
        this.latch = latch;
        this.checkLastModified = checkLastModified;
    }

    public boolean getStopScraping() {
        return stopScraping;
    }

    public void setStopScraping(boolean stopScraping) {
        this.stopScraping = stopScraping;
    }

    @Override
    public void run() {
        while (!stopScraping) {
            try {
                Link currentLink = urlQueue.take();
                if (currentLink.level > Constants.crawlMaxDepth) break;

                // Start to crawl from the currentLink
                Response res = null;
                res = CrawlerHelper.getResponse(currentLink.url, urls);

                int size = res.bodyAsBytes().length;
                String lastModified = res.header("last-modified");
                Document currentDoc = res.parse();
                Vector<String> links = CrawlerHelper.extractLinks(currentDoc);

                if (checkLastModified) {
                    int pageId = URLToPageId.getValue(currentLink.url);
                    ZonedDateTime converted_lastModified =  ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME);

                    if(pageId >= 0 && converted_lastModified == PageIdToData.getValue(pageId).getLastModified()){
                        continue;
                    }
                }

                CrawlerHelper.extractAndPushChildLinksFromParentToUrlQueue(currentLink, links, urlQueue, urls);
                HashMap<String, ArrayList<Integer>> wordToWordLocations = CrawlerHelper.extractCleanedWordLocationsMap(
                    currentDoc
                );

                HashMap<String, ArrayList<Integer>> titleWordToWordLocations = CrawlerHelper.extractCleanedTitleWordLocationsMap(
                    currentDoc
                );

                int tfmax = CrawlerHelper.getTfmax(wordToWordLocations);
                Page pageData = CrawlerHelper.extractPageData(size, lastModified, currentDoc, links, tfmax, currentLink.url);

                // pageData
                int pageId = RocksDBApi.addPageData(pageData, currentLink.url);

                // inverted index
                ArrayList<Integer> wordIds = RocksDBApi.addPageWords(wordToWordLocations, pageId);

                // title inverted index
                RocksDBApi.addPageTitleToTitleInvertedIndex(titleWordToWordLocations, pageId);

                // forward index
                ForwardIndex.addEntry(pageId, wordIds);

                System.out.println("Indexed: " + currentLink.url);

            } catch (Exception e) {
                System.out.println(e + " caught");
            }
        }
        System.out.println("Counting down the latch");
        latch.countDown();
    }
}
