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
        TitleInvertedIndex.connect();
        PageIdToParentIds.connect();
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
        TitleInvertedIndex.deleteAll();
        PageIdToParentIds.deleteAll();
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
     *
     */
    public synchronized static ArrayList<Integer> addPageWords(Map<String, ArrayList<Integer>> wordToLocsMap, int pageId)
        throws RocksDBException, InvalidWordIdConversionException {

        ArrayList<Integer> wordIds = new ArrayList<>();
        InvertedIndex.createEntriesInBatch(createHashMapForInvertedFileBatchEntry(wordToLocsMap, pageId, wordIds));

        return wordIds;
    }

    public synchronized static void addPageTitleToTitleInvertedIndex(Map<String, ArrayList<Integer>> titleWordToLocsMap, int pageId)
        throws RocksDBException, InvalidWordIdConversionException {

        TitleInvertedIndex.createEntriesInBatch(createHashMapForInvertedFileBatchEntry(titleWordToLocsMap, pageId, null));
    }

    private synchronized static HashMap<byte[], ArrayList<Integer>> createHashMapForInvertedFileBatchEntry(Map<String, ArrayList<Integer>> wordToLocsMap, int pageId, ArrayList<Integer> wordIds)
        throws RocksDBException, InvalidWordIdConversionException {

        byte[] key;
        String keyword;
        int wordId;

        HashMap<byte[], ArrayList<Integer>> invertedIndexBatchEntries = new HashMap<>();

        for (Entry<String, ArrayList<Integer>> iterator : wordToLocsMap.entrySet()) {
            keyword = iterator.getKey();

            wordId = WordToWordId.getValue(keyword);
            if (wordId == -1) {
                wordId = Metadata.getLatestIndex(Metadata.Key.WORD);
                WordToWordId.addEntry(keyword, wordId);
            }

            key = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
            ArrayList<Integer> locations = iterator.getValue();

            invertedIndexBatchEntries.put(key, locations);

            if (wordIds != null) wordIds.add(wordId);
        }
        return invertedIndexBatchEntries;
    }


    /**
     * @return key: pageId string, value: word locations array list
     */
    public static HashMap<Integer, ArrayList<Integer>> getWordValues(String word)
        throws RocksDBException, InvalidWordIdConversionException {
        int wordId = WordToWordId.getValue(word);
        if (wordId == -1) return null;
        String prefix = WordUtilities.buildDBKeyPrefix(wordId).toString();
        return InvertedIndex.getValue(prefix.getBytes());
    }

    public static HashMap<Integer, Double> getPageWordWeights(int pageId) throws InvalidWordIdConversionException {
        String prefix = WordUtilities.buildDBKeyPrefix(pageId).toString();
        return WeightIndex.getValue(prefix.getBytes());
    }

    public static ArrayList<Integer> getPageIdsOfWord(String word) throws
        RocksDBException, InvalidWordIdConversionException {
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

    public static ArrayList<Integer> getInvertedValuesFromKey(int wordId, int pageId) throws
        InvalidWordIdConversionException, RocksDBException {
        byte[] key = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
        return InvertedIndex.getValueByKey(key);
    }

    public static HashMap<Integer, Page> getAllPageData() throws ClassNotFoundException, IOException {
        return PageIdToData.getAll();
    }

    public static Integer getPageIdFromURL(String url) throws RocksDBException {
        return URLToPageId.getValue(url);
    }

    public static HashMap<Integer, ArrayList<Integer>> getTitleWordValues(String word)
        throws RocksDBException, InvalidWordIdConversionException {
        int wordId = WordToWordId.getValue(word);
        if (wordId == -1) return null;
        String prefix = WordUtilities.buildDBKeyPrefix(wordId).toString();
        return TitleInvertedIndex.getValue(prefix.getBytes());
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
        TitleInvertedIndex.closeConnection();
        PageIdToParentIds.closeConnection();
    }
}
