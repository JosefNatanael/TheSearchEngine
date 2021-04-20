package comp4321.group2.searchengine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.rocksdb.RocksDBException;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.repositories.*;
import comp4321.group2.searchengine.repositories.InvertedIndex;
import comp4321.group2.searchengine.repositories.Metadata;
import comp4321.group2.searchengine.repositories.PageIdToData;
import comp4321.group2.searchengine.repositories.URLToPageId;
import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.springframework.web.filter.ForwardedHeaderFilter;

public final class RocksDBApi {

    public static void connect() throws RocksDBException {
        File directory = new File("./src/main/java/tables/");
        if (!directory.exists()) {
            directory.mkdir();
        }
        InvertedIndex.connect();
        Metadata.connect();
        PageIdToData.connect();
        URLToPageId.connect();
        WordToWordId.connect();
        WordIdToIdf.connect();
    }

    public static void reset() throws RocksDBException {
        InvertedIndex.deleteAll();
        Metadata.deleteAll();
        PageIdToData.deleteAll();
        URLToPageId.deleteAll();
        WordToWordId.deleteAll();
        WordIdToIdf.deleteAll();
    }

    /**
     * Get pageId for url. If does not exist, will return -1;
     * @param url
     * @return pageId integer
     * @throws RocksDBException
     */
    public static int getPageIdFromUrl(String url) throws RocksDBException {
        int index = URLToPageId.getValue(url);
        return index;
    }

    /**
     * call this function in crawler to store page
     * @return pageIndex integer
     * @throws IOException
     */
    public static int addPageData(Page page, String url) throws RocksDBException, IOException {
        // 1. Using URL, check in URLToPageId for index
        int index = getPageIdFromUrl(url);
        if (index == -1) {
            int indexInt = Metadata.getLatestIndex(Metadata.Key.PAGE);
            URLToPageId.addEntry(url, indexInt);
            index = indexInt;
        }

        // 2. Using index, update in PageIdToData
        PageIdToData.addEntry(index, page);

        return index;
    }

    /**
     *
     * @param wordToLocsMap
     * @param pageId
     * @throws InvalidWordIdConversionException
     */
    public static void addPageWords(Map<String, ArrayList<Integer>> wordToLocsMap, int pageId)
        throws RocksDBException, IOException, InvalidWordIdConversionException {

        byte[] key;

        String keyword;
        int wordId;

        for (Entry<String, ArrayList<Integer>> iterator : wordToLocsMap.entrySet()) {
            keyword = iterator.getKey();

            wordId = WordToWordId.getValue(keyword);
            if (wordId == -1) {
                wordId = Metadata.getLatestIndex(Metadata.Key.WORD);
                WordToWordId.addEntry(keyword, wordId);
            }

            ArrayList<Integer> locations = iterator.getValue();
            byte[] values = WordUtilities.arrayListToString(locations).getBytes();

            key = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
            InvertedIndex.addEntry(key, values);
        }
    }

    public static void addIdf(int key,  double value)
        throws RocksDBException{
        WordIdToIdf.addEntry(key, value);
    }



    /**
     *
     * @param word
     * @return key: pageId string, value: word locations array list
     * @throws RocksDBException
     * @throws InvalidWordIdConversionException
     */
    public static HashMap<String, ArrayList<Integer>> getWordValues(String word)
        throws RocksDBException, InvalidWordIdConversionException {
        int wordId = WordToWordId.getValue(word);
        if (wordId == -1) return null;
        String prefix = WordUtilities.buildDBKeyPrefix(wordId).toString();
        return InvertedIndex.getValue(prefix.getBytes());
    }

    public static ArrayList<Integer> getPageIdsOfWord(String word) throws RocksDBException, InvalidWordIdConversionException {
        int wordId = WordToWordId.getValue(word);
        if (wordId == -1) return null;
        String prefix = WordUtilities.buildDBKeyPrefix(wordId).toString();
        return InvertedIndex.getPageIds(prefix.getBytes());
    }

    public static Page getPageData(String url) throws RocksDBException, IOException, ClassNotFoundException {
        int index = getPageIdFromUrl(url);
        if (index == -1) return null;
        Page pageData = PageIdToData.getValue(index);
        return pageData;
    }

    public static HashMap<String, Integer> getAllWordToWordID() throws RocksDBException{
        return WordToWordId.getAll();
    }

    public static HashMap<String, Integer> getAllMetadata() throws RocksDBException{
        return Metadata.getAll();
    }

    public static HashMap<Integer, Double> getPageWordWeights(int pageId) throws RocksDBException, IOException, ClassNotFoundException {
        HashMap<Integer, Integer> wordIdToTf = ForwardIndex.getValue(pageId);
        Page pageData = PageIdToData.getValue(pageId);
        int tfMax = pageData.getTfmax();

        HashMap<Integer, Double> wordIdToWeight = new HashMap<Integer, Double>();

        for (Entry<Integer, Integer> entry : wordIdToTf.entrySet()) {
            int wordId = entry.getKey();
            int tf = entry.getValue();

            double idf = WordIdToIdf.getValue(wordId);
            Double weight = Double.valueOf(tf) * idf / Double.valueOf(tfMax);
            wordIdToWeight.put(wordId, weight);
        }

        return wordIdToWeight;
    }

    public static int getWordIdOfWord(String word) throws RocksDBException {
        return WordToWordId.getValue(word);
    }

    public static double getPageLength(int pageId) {
        return 0.0;
    }

    public static void closeAllDBConnections() {
        InvertedIndex.closeConnection();
        Metadata.closeConnection();
        PageIdToData.closeConnection();
        URLToPageId.closeConnection();
        WordToWordId.closeConnection();
        WordIdToIdf.closeConnection();
    }
}
