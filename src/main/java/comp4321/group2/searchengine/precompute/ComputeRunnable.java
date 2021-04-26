package comp4321.group2.searchengine.precompute;

import comp4321.group2.searchengine.crawler.Link;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class ComputeRunnable implements Runnable {

    private final CountDownLatch latch;
    private volatile boolean stopScraping = false;

    public ComputeRunnable(BlockingQueue<Link> urlQueue, Set<String> urls, CountDownLatch latch) {
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
//        while (!stopScraping) {
//            try {
//                Link currentLink = urlQueue.take();
//                if (currentLink.level > Constants.crawlMaxDepth) break;
//                if (urls.contains(currentLink.url)) continue;
//
//                // Start to crawl from the currentLink
//                Connection.Response res = null;
//                res = CrawlerHelper.getResponse(currentLink.url, urls);
//
//                int size = res.bodyAsBytes().length;
//                String lastModified = res.header("last-modified");
//                Document currentDoc = res.parse();
//                Vector<String> links = CrawlerHelper.extractLinks(currentDoc);
//
//                CrawlerHelper.extractAndPushChildLinksFromParentToUrlQueue(currentLink, links, urlQueue);
//                HashMap<String, ArrayList<Integer>> wordToWordLocations = CrawlerHelper.extractCleanedWordLocationsMapFromDocument(
//                    currentDoc
//                );
//
//                int tfmax = CrawlerHelper.getTfmax(wordToWordLocations);
//                Page pageData = CrawlerHelper.extractPageData(size, lastModified, currentDoc, links, tfmax);
//
//                //pageData
//                int pageId = RocksDBApi.addPageData(pageData, currentLink.url);
//
//                //inverted index
//                ArrayList<Integer> wordIds = RocksDBApi.addPageWords(wordToWordLocations, pageId);
//
//                //forward index
//                ForwardIndex.addEntry(pageId, wordIds);
//
//                System.out.println("Indexed: " + currentLink.url);
//
//            } catch (InterruptedException ignore) {
//                System.out.println("InterruptedException caught");
//            } catch (HttpStatusException ignore) {
//                System.out.println("HttpStatusException caught");
//            } catch(SocketTimeoutException ignore) {
//                System.out.println("SocketTimeoutException caught");
//            } catch (IOException ignore) {
//                System.out.println("IOException caught");
//            } catch (RocksDBException ignore) {
//                System.out.println("RocksDBException caught");
//            } catch (InvalidWordIdConversionException ignore) {
//                System.out.println("InvalidWordIdConversionException caught");
//            } catch (RevisitException ignore) {
//                System.out.println("RevisitException caught");
//            } catch (UncheckedIOException ignore) {
//                System.out.println("UncheckedIOException caught");
//            }
//            catch (Exception e) {
//                System.out.println("Some exception: " + e.toString());
//            }
//        }
        System.out.println("Counting down the latch");
        latch.countDown();
    }
}
