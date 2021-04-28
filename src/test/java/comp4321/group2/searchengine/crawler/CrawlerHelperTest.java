package comp4321.group2.searchengine.crawler;

import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class CrawlerHelperTest {

    @Test
    void extractAndPushChildLinksFromParentToUrlQueue() {
        String childLinkString1 = "some/link/#/";
        String childLinkString2 = "another/link/#";
        String childLinkString3 = "some/other/link///";
        String childLinkString4 = "and/another/link";
        String childLinkString5 = "and/another/link/";

        Link parentLink = new Link("parent/url", 1);
        Vector<String> links = new Vector<>();
        BlockingQueue<Link> urlQueue = new LinkedBlockingQueue<>();
        Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

        links.add(childLinkString1);
        links.add(childLinkString2);
        links.add(childLinkString3);
        links.add(childLinkString4);
        links.add(childLinkString5);

        CrawlerHelper.extractAndPushChildLinksFromParentToUrlQueue(parentLink, links, urlQueue, visitedUrls);

        urlQueue.forEach(link -> System.out.println(link.url));
//        assertTrue(urlQueue.size() > 0);
        System.out.println("Hello");
    }
}
