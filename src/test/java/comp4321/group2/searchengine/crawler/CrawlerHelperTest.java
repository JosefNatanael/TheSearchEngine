package comp4321.group2.searchengine.crawler;

import comp4321.group2.searchengine.utils.StopStem;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class CrawlerHelperTest {

    @Test
    void extractAndPushChildLinksFromParentToUrlQueue() {
        String childLinkString1 = "cse.ust.hk/some/link/#/";
        String childLinkString2 = "ancse.ust.hkother/link/#";
        String childLinkString3 = "somcse.ust.hke/other/link///";
        String childLinkString4 = "acse.ust.hknd/another/link";
        String childLinkString5 = "ancse.ust.hkd/another/link/";

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
        assertTrue(urlQueue.size() > 0);
    }

    @Test
    void extractWordsTest() {
        Vector<String> result = new Vector<>();

        String contents = "The department offers two major programs. COMP is our general undergraduate degree program. It provides a broad education in all core areas of Computer Science, while allowing students the flexibility to pursue individual interests in higher-level areas. COSC is a special program catered only to students wishing to double-major with other degrees. Thus, it offers more flexibility in fulfilling the requirements. Additionally, we offer interdisciplinary joint programs with other departments. For details on all of these programs, please refer to the links below.";
        StringTokenizer st = new StringTokenizer(contents);
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }

        result.forEach(word -> System.out.println(StopStem.stem(word)));
    }
}
