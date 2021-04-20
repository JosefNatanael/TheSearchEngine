package comp4321.group2.searchengine.crawler;


import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.HttpStatusException;
import org.jsoup.nodes.Document;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class CrawlerRunnable implements Runnable {
    private BlockingQueue<Link> urlQueue;
    private Set<String> urls;
    private CountDownLatch latch;
    private boolean condition = true;
    private volatile boolean stopScraping = false;

    public CrawlerRunnable(BlockingQueue<Link> urlQueue, Set<String> urls, CountDownLatch latch) {
        this.urlQueue = urlQueue;
        this.urls = urls;
        this.latch = latch;
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
                if (urls.contains(currentLink.url)) continue;

                // Start to crawl from the currentLink
                Response res = CrawlerHelper.getResponse(currentLink.url, urls);
                int size = res.bodyAsBytes().length;
                String lastModified = res.header("last-modified");
                Document currentDoc = res.parse();
                Vector<String> links = CrawlerHelper.extractLinks(currentDoc);

                CrawlerHelper.extractAndPushChildLinksFromParentToUrlQueue(currentLink, links, urlQueue);
                HashMap<String, ArrayList<Integer>> wordToWordLocations = CrawlerHelper.extractCleanedWordLocationsMapFromDocument(
                    currentDoc
                );

                int tfmax = CrawlerHelper.getTfmax(wordToWordLocations);
                Page pageData = CrawlerHelper.extractPageData(size, lastModified, currentDoc, links, tfmax);
                int pageId = RocksDBApi.addPageData(pageData, currentLink.url);
                RocksDBApi.addPageWords(wordToWordLocations, pageId);

                System.out.println("Indexed a page");

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (HttpStatusException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RocksDBException e) {
                e.printStackTrace();
            } catch (InvalidWordIdConversionException e) {
                e.printStackTrace();
            }
        }
        latch.countDown();
    }
}
