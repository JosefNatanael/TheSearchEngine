package comp4321.group2.searchengine.crawler;

import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.utils.StopStem;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

abstract class CrawlerHelper {
    private static final File stopwordsPath = new File("./src/main/resources/stopwords.txt");
    private static final StopStem stopStem = new StopStem(stopwordsPath.getAbsolutePath());
    private static final String[] blackListLinkStartsWith = {"https://www.cse.ust.hk/Restricted"};
    private static final String[] blackListLinkEndsWith = {"\\/#$", "\\/#\\/$", "[\\/]+$"};

    /**
     * Send an HTTP request and analyze the response.
     *
     * @return Response res
     */
    public static Response getResponse(String url, Set<String> visitedUrls) throws IOException {
        if (visitedUrls.contains(url)) {
            throw new RevisitException(); // if the page has been visited, break the function
        }

        Connection conn = Jsoup.connect(url).followRedirects(false).timeout(Constants.millisTimeout).maxBodySize(0);
        // the default body size is 2Mb, to attain unlimited page, use the following
        // Connection conn = Jsoup.connect(this.url).maxBodySize(0).followRedirects(false);
        Response res;
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
            visitedUrls.add(url);
            throw e;
        } finally {
            visitedUrls.add(url);
        }
        return res;
    }

    /**
     * Extract words in the web page content.
     * note: use StringTokenizer to tokenize the result
     *
     * @return Vector<String> a list of words in the web page body
     */
    public static Vector<String> extractWords(Document doc) {
        Vector<String> result = new Vector<>();
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

    /**
     * Extract title words from document
     */
    public static Vector<String> extractTitle(Document doc) {
        Vector<String> result = new Vector<>();
        if (doc == null || doc.title() == null || doc.title() == null) {
            return result;
        }
        String contents = doc.title();
        StringTokenizer st = new StringTokenizer(contents);
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;
    }

    /**
     * Extract useful external urls on the web page.
     * note: filter out images, emails, etc.
     *
     * @return Vector<String> a list of external links on the web page
     */
    public static Vector<String> extractLinks(Document doc) {
        Vector<String> result = new Vector<>();
        Elements links = doc.select("a[href]");
        for (Element link : links) {
            String linkString = link.absUrl("href");
            if (linkString.contains("mailto:") || linkString.isEmpty() || linkString.startsWith("#") || linkString.startsWith("javascript")) {
                continue;
            }
            result.add(linkString);
        }
        return result;
    }

    /**
     * @return tfmax
     */
    public static int getTfmax(Map<String, ArrayList<Integer>> wordToWordLocations) {
        int tfmax = 0;
        int tf = 0;

        for (Map.Entry<String, ArrayList<Integer>> pair : wordToWordLocations.entrySet()) {
            List<Integer> locations = pair.getValue();
            tf = locations.size();
            if (tf > tfmax) tfmax = tf;
        }
        return tfmax;
    }

    /**
     * Extract child links from current (parent) page link
     *
     * @param parentLink Parent Link
     * @param links      Child Links
     */
    public static void extractAndPushChildLinksFromParentToUrlQueue(Link parentLink, Vector<String> links, Queue<Link> urlQueue, Set<String> visitedUrls) {
        // Add child links to urlQueue vector
        for (String link : links) {
            if (link.contains("cse.ust.hk")) {
                boolean skipFlag = false;
                // Check if the current link is in the blacklist, skip it!
                for (String blacklist : blackListLinkStartsWith) {
                    if (link.startsWith(blacklist)) {
                        skipFlag = true;
                        break;
                    }
                }

                for (String blacklist : blackListLinkEndsWith) {
                    if (Pattern.matches("[\\w\\W]+\\w" + blacklist, link)) {
                        link = link.replaceAll("([\\w\\W]+\\w)" + blacklist, "$1/");
                    }
                }

                if (!link.endsWith("/")) {
                    link += "/";
                }
                if (skipFlag || visitedUrls.contains(link)) continue;
                urlQueue.add(new Link(link, parentLink.level + 1)); // add links to urlQueue vector
            }
        }
    }

    public static Page extractPageData(int size, String lastModified, Document doc, Vector<String> links, int tfmax, String url) {
        /* Get the metadata from the result */
        String title = doc.title();
        String urls = WordUtilities.arrayListToString(new ArrayList<>(links));
        return new Page(title, urls, size, lastModified, tfmax, url);
    }

    /**
     * Clean: stopwords removed, stemmed with porter
     * Extracts the word locations for each word in current document page
     *
     * @return HashMap<String, ArrayList < Integer>> key: word string, value: array list of integer locations
     */
    public static HashMap<String, ArrayList<Integer>> extractCleanedWordLocationsMap(Document document) {
        Vector<String> words = extractWords(document);
        HashMap<String, ArrayList<Integer>> wordToWordLocationMap = new HashMap<>();
        for (int i = 0; i < words.size(); ++i) {
            String currWord = words.get(i);
            currWord = currWord.replaceAll("\\d", "");
            String stemmedWord = stopStem.stem(currWord);
            if (stopStem.isStopWord(currWord) || stemmedWord.equals("")) {
                continue;
            }
            if (wordToWordLocationMap.containsKey(stemmedWord)) {
                wordToWordLocationMap.get(stemmedWord).add(i);
            } else {
                ArrayList<Integer> arrList = new ArrayList<>();
                arrList.add(i);
                wordToWordLocationMap.put(stemmedWord, arrList);
            }
        }
        return sortWordLocationsHashMap(wordToWordLocationMap);
    }

    public static HashMap<String, ArrayList<Integer>> extractCleanedTitleWordLocationsMap(Document document) {
        Vector<String> titleWords = extractTitle(document);
        HashMap<String, ArrayList<Integer>> titleWordToLocationsMap = new HashMap<>();
        for (int i = 0; i < titleWords.size(); ++i) {
            String currWord = titleWords.get(i);
            currWord = currWord.toLowerCase();

            if (titleWordToLocationsMap.containsKey(currWord)) {
                titleWordToLocationsMap.get(currWord).add(i);
            } else {
                ArrayList<Integer> arrList = new ArrayList<>();
                arrList.add(i);
                titleWordToLocationsMap.put(currWord, arrList);
            }
        }
        return sortWordLocationsHashMap(titleWordToLocationsMap);
    }

    private static HashMap<String, ArrayList<Integer>> sortWordLocationsHashMap(HashMap<String, ArrayList<Integer>> wordToLocationsMap) {
        for (Map.Entry<String, ArrayList<Integer>> pair : wordToLocationsMap.entrySet()) {
            ArrayList<Integer> locations = pair.getValue();
            Collections.sort(locations);
            wordToLocationsMap.put(pair.getKey(), locations);
        }

        return wordToLocationsMap;
    }
}
