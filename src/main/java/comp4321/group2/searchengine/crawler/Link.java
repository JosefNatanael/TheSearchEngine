package comp4321.group2.searchengine.crawler;

/**
 * The data structure for the crawling queue.
 */
public class Link {

    final String url;
    final int level;

    Link(String url, int level) {
        this.url = url;
        this.level = level;
    }
}
