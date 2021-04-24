package comp4321.group2.searchengine.common;

public final class Constants {

    private Constants() {}

    // Prefix settings
    public static final int prefixBytesLength = 8;

    // Crawler settings
    public static final int crawlMaxDepth = Integer.MAX_VALUE;
    public static final boolean limitNumberOfPagesToScrape = true;
    public static final int numberOfPagesToScrape = 30;
    public static final int numCrawlerThreads = 100;
    public static final int millisTimeout = 3000;

    // Database names
    public static final String pageIdToDataName = "pageIdToDataDB";
    public static final String invertedIndexName = "invertedIndexDB";
    public static final String metadataName = "metadataDB";
    public static final String urlToPageIdName = "urlToPageIdDB";
    public static final String wordToWordIdName = "wordToWordIdDB";

    // Database folder root
    public static final String databaseRootDir = "./src/main/java/tables/";
}
