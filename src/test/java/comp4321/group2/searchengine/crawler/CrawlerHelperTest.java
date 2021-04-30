package comp4321.group2.searchengine.crawler;

import org.jsoup.Connection;
import org.jsoup.nodes.Document;
import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class CrawlerHelperTest {

    private Document sampleDocument;

    @BeforeEach
    public void starter() throws IOException {
        Set<String> visitedUrls = new HashSet<>();
        String url = "https://www.cse.ust.hk/";
        Connection.Response res = CrawlerHelper.getResponse(url, visitedUrls);
        sampleDocument = res.parse();
    }

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
    void getResponseTester() throws IOException {
        Set<String> visitedUrls = new HashSet<>();
        String url = "https://www.cse.ust.hk/";
        Connection.Response res = CrawlerHelper.getResponse(url, visitedUrls);
        int size = res.bodyAsBytes().length;
        Document currentDoc = res.parse();
        assertTrue(size > 0);
        assertNotNull(currentDoc);
    }

    @Test
    void extractWordsTest() {
        Vector<String> result = new Vector<>();
        String[] expectedArray = {"The", "department", "offers", "two", "major", "programs.", "COMP", "is", "our", "general."};
        Vector<String> expected = new Vector<String>(Arrays.asList(expectedArray));

        String contents = "The department offers two major programs. COMP is our general.";
        StringTokenizer st = new StringTokenizer(contents);
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }

        for (int i = 0; i < result.size(); ++i) {
            assertEquals(expected.get(i), result.get(i));
        }
    }

    @Test
    void extractTitle() {
        Vector<String> vec = CrawlerHelper.extractTitle(sampleDocument);
        String[] exp = {"Department", "of", "Computer", "Science", "and", "Engineering", "-", "HKUST"};
        assertArrayEquals(exp, vec.toArray());
    }

    @Test
    void extractCleanedTitleWordLocationsMap() {
        HashMap<String, ArrayList<Integer>> result = CrawlerHelper.extractCleanedTitleWordLocationsMap(sampleDocument);
        HashMap<String, ArrayList<Integer>> exp = new HashMap<>();
        exp.put("computer", new ArrayList<Integer>(){{add(2);}});
        exp.put("and", new ArrayList<Integer>(){{add(4);}});
        exp.put("of", new ArrayList<Integer>(){{add(1);}});
        exp.put("science", new ArrayList<Integer>(){{add(3);}});
        exp.put("hkust", new ArrayList<Integer>(){{add(7);}});
        exp.put("engineering", new ArrayList<Integer>(){{add(5);}});
        exp.put("department", new ArrayList<Integer>(){{add(0);}});
        exp.put("-", new ArrayList<Integer>(){{add(6);}});
        assertEquals(result, exp);
    }
}
