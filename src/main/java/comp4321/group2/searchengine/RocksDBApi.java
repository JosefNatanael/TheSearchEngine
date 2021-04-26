package comp4321.group2.searchengine;

import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.repositories.*;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
        ForwardIndex.connect();
        PageIdToLength.connect();
        WeightIndex.connect();
    }

    public static void reset() throws RocksDBException {
        InvertedIndex.deleteAll();
        Metadata.deleteAll();
        PageIdToData.deleteAll();
        URLToPageId.deleteAll();
        WordToWordId.deleteAll();
        WordIdToIdf.deleteAll();
        ForwardIndex.deleteAll();
        PageIdToLength.deleteAll();
        WeightIndex.deleteAll();
    }

    /**
     * call this function in crawler to store page
     *
     * @return pageIndex integer
     */
    public static int addPageData(Page page, String url) throws RocksDBException, IOException {
        // 1. Using URL, check in URLToPageId for index
        int index = URLToPageId.getValue(url);
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
     */
    public static ArrayList<Integer> addPageWords(Map<String, ArrayList<Integer>> wordToLocsMap, int pageId)
        throws RocksDBException, InvalidWordIdConversionException {

        byte[] key;
        String keyword;
        int wordId;
        ArrayList<Integer> wordIds = new ArrayList<>();

        for (Entry<String, ArrayList<Integer>> iterator : wordToLocsMap.entrySet()) {
            keyword = iterator.getKey();

            wordId = WordToWordId.getValue(keyword);
            if (wordId == -1) {
                wordId = Metadata.getLatestIndex(Metadata.Key.WORD);
                WordToWordId.addEntry(keyword, wordId);
            }
            wordIds.add(wordId);

            ArrayList<Integer> locations = iterator.getValue();
            byte[] values = WordUtilities.arrayListToString(locations).getBytes();

            key = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
            InvertedIndex.addEntry(key, values);
        }
        return wordIds;
    }


    /**
     * @return key: pageId string, value: word locations array list
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
        int index = URLToPageId.getValue(url);
        if (index == -1) return null;
        return PageIdToData.getValue(index);
    }

    public static ArrayList<Integer> getInvertedValuesFromKey(int wordId, int pageId) throws InvalidWordIdConversionException, RocksDBException {
        byte[] key = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
        return InvertedIndex.getValueByKey(key);
    }

    public static HashMap<Integer, Double> getPageWordWeights(int pageId) throws RocksDBException, IOException, ClassNotFoundException, InvalidWordIdConversionException {
        ArrayList<Integer> wordIds = ForwardIndex.getValue(pageId);
        Page pageData = PageIdToData.getValue(pageId);
        int tfMax = pageData.getTfmax();

        HashMap<Integer, Double> wordIdToWeight = new HashMap<>();

        for (int wordId : wordIds) {
            byte[] key = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
            int tf = InvertedIndex.getValueByKey(key).size();

            double idf = WordIdToIdf.getValue(wordId);
            Double weight = (double) tf * idf / (double) tfMax;
            wordIdToWeight.put(wordId, weight);
        }

        return wordIdToWeight;
    }

    public static void closeAllDBConnections() {
        InvertedIndex.closeConnection();
        Metadata.closeConnection();
        PageIdToData.closeConnection();
        URLToPageId.closeConnection();
        WordToWordId.closeConnection();
        WordIdToIdf.closeConnection();
        ForwardIndex.closeConnection();
        PageIdToLength.closeConnection();
        WeightIndex.closeConnection();
    }
}
