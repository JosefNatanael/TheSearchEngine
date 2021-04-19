package comp4321.group2.searchengine;

import java.io.File;
import java.io.IOException;
import java.lang.RuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.Vector;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.rocksdb.RocksDBException;

import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.utils.StopStem;
import comp4321.group2.searchengine.utils.WordUtilities;

/** The data structure for the crawling queue.
 */
class Link {

    String url;
    int level;

    Link(String url, int level) {
        this.url = url;
        this.level = level;
    }
}

@SuppressWarnings("serial")
/** This is customized exception for those pages that have been visited before.
 */
class RevisitException extends RuntimeException {

    public RevisitException() {
        super();
    }
}

public final class Crawler {

    private HashSet<String> urls; // the set of urls that have been visited before
    public Vector<Link> urlQueue; // the queue of URLs to be crawled
    private int crawlMaxDepth = Constants.crawlMaxDepth; // change the depth limit of the spider.
    private StopStem stopStem;
    private File stopwordsPath = new File("./src/main/resources/stopwords.txt");

    public Crawler(String _url) throws RocksDBException {
        urlQueue = new Vector<Link>();
        urlQueue.add(new Link(_url, 1));
        urls = new HashSet<String>();
        stopStem = new StopStem(stopwordsPath.getAbsolutePath());
    }

    /**
     * Send an HTTP request and analyze the response.
     * @return Response res
     * @throws HttpStatusException for non-existing pages
     * @throws IOException
     */
    public Response getResponse(String url) throws HttpStatusException, IOException {
        if (this.urls.contains(url)) {
            throw new RevisitException(); // if the page has been visited, break the function
        }

        Connection conn = Jsoup.connect(url).followRedirects(false);
        // the default body size is 2Mb, to attain unlimited page, use the following
        // Connection conn = Jsoup.connect(this.url).maxBodySize(0).followRedirects(false);
        Response res;
        try {
            /* establish the connection and retrieve the response */
            res = conn.execute();
            /* if the link redirects to other place... */
            if (res.hasHeader("location")) {
                String actual_url = res.header("location");
                if (this.urls.contains(actual_url)) {
                    throw new RevisitException();
                } else {
                    this.urls.add(actual_url);
                }
            } else {
                this.urls.add(url);
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
    public Vector<String> extractWords(Document doc) {
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
    public Vector<String> extractLinks(Document doc) {
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
    public void indexToDB() throws RocksDBException, InvalidWordIdConversionException {
        int iterationNumber = 0;
        int numberOfPagesToScrape = Constants.numberOfPagesToScrape;

        while (
            !this.urlQueue.isEmpty() && Constants.limitNumberOfPagesToScrape && iterationNumber < numberOfPagesToScrape
        ) {
            System.out.println("Iteration number: " + Integer.toString(iterationNumber++));
            Link currentLink = this.urlQueue.remove(0);
            if (currentLink.level > this.crawlMaxDepth) break; // stop criteria
            if (this.urls.contains(currentLink.url)) continue; // ignore pages that has been visited

            /* start to crawl on the page */
            try {
                Response res = this.getResponse(currentLink.url);
                int size = res.bodyAsBytes().length;
                String lastModified = res.header("last-modified");
                Document currentDoc = res.parse();
                Vector<String> links = this.extractLinks(currentDoc);

                extractAndPushChildLinksFromParentToUrlQueue(currentLink, links);
                HashMap<String, ArrayList<Integer>> wordToWordLocations = extractCleanedWordLocationsMapFromDocument(
                    currentDoc
                );

                int tfmax = getTfmax(wordToWordLocations);
                Page pageData = extractPageData(size, lastModified, currentDoc, links, tfmax);
                int pageId = RocksDBApi.addPageData(pageData, currentLink.url);
                RocksDBApi.addPageWords(wordToWordLocations, pageId);
            } catch (HttpStatusException e) {
                e.printStackTrace();
                System.out.printf("\nLink Error: %s\n", currentLink.url);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RevisitException e) {}
        }
    }

    /**
     *
     * @param wordToWordLocations
     * @return tfmax
     */
    public int getTfmax(HashMap<String, ArrayList<Integer>> wordToWordLocations){
        int tfmax = 0;
        int tf=0;

        for (Entry<String, ArrayList<Integer>> pair : wordToWordLocations.entrySet()) {
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
    public void extractAndPushChildLinksFromParentToUrlQueue(Link parentLink, Vector<String> links)
        throws HttpStatusException, IOException {
        // Add child links to urlQueue vector
        for (String link : links) {
            this.urlQueue.add(new Link(link, parentLink.level + 1)); // add links to urlQueue vector
        }
    }


    public Page extractPageData(int size, String lastModified, Document doc, Vector<String> links, int tfmax) throws IOException {
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
    public HashMap<String, ArrayList<Integer>> extractCleanedWordLocationsMapFromDocument(Document document) {
        Vector<String> words = this.extractWords(document);
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
        for (Entry<String, ArrayList<Integer>> pair : wordToWordLocationMap.entrySet()) {
            ArrayList<Integer> locations = pair.getValue();
            Collections.sort(locations);
            wordToWordLocationMap.put(pair.getKey(), locations);
        }

        return wordToWordLocationMap;
    }
}
