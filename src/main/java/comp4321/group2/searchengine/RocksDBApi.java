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
    }

    public static void reset() throws RocksDBException {
        InvertedIndex.deleteAll();
        Metadata.deleteAll();
        PageIdToData.deleteAll();
        URLToPageId.deleteAll();
        WordToWordId.deleteAll();
    }

    /**
     * Get pageId for url. If does not exist, will return -1;
     * @param url
     * @return pageId integer
     * @throws RocksDBException
     */
    public static int getPageIdFromUrl(String url) throws RocksDBException {
        byte[] urlBytes = url.getBytes();
        byte[] index = URLToPageId.getValue(urlBytes);
        if (index != null) {
            return ByteIntUtilities.convertByteArrayToInt(index);
        } else {
            return -1;
        }
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
            URLToPageId.addEntry(url.getBytes(), ByteIntUtilities.convertIntToByteArray(indexInt));
            index = indexInt;
        }

        // 2. Using index, update in PageIdToData
        byte[] pageByte = Page.serialize(page);
        PageIdToData.addEntry(ByteIntUtilities.convertIntToByteArray(index), pageByte);

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
        byte[] wordId;
        byte[] key;
        byte[] wordBytes;
        String keyword;
        int wordIdInt;

        for (Entry<String, ArrayList<Integer>> iterator : wordToLocsMap.entrySet()) {
            keyword = iterator.getKey();
            wordBytes = keyword.getBytes();

            wordId = WordToWordId.getValue(wordBytes);
            if (wordId != null) {
                wordIdInt = ByteIntUtilities.convertByteArrayToInt(wordId);
            } else {
                wordIdInt = Metadata.getLatestIndex(Metadata.Key.WORD);
                WordToWordId.addEntry(wordBytes, ByteIntUtilities.convertIntToByteArray(wordIdInt));
            }

            ArrayList<Integer> locations = iterator.getValue();
            byte[] values = WordUtilities.arrayListToString(locations).getBytes();

            key = WordUtilities.wordIdAndPageIdToDBKey(wordIdInt, pageId);
            InvertedIndex.addEntry(key, values);
        }
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
        byte[] idBytes = WordToWordId.getValue(word.getBytes());
        if (idBytes == null) return null;
        int wordId = ByteIntUtilities.convertByteArrayToInt(idBytes);
        String prefix = WordUtilities.buildDBKeyPrefix(wordId).toString();
        return InvertedIndex.getValue(prefix.getBytes());
    }

    public static Page getPageData(String url) throws RocksDBException, IOException, ClassNotFoundException {
        int index = getPageIdFromUrl(url);
        if (index == -1) return null;
        byte[] pageDataBytes = PageIdToData.getValue(ByteIntUtilities.convertIntToByteArray(index));
        if (pageDataBytes == null) return null;
        Page pageData = Page.deserialize(pageDataBytes);
        return pageData;
    }

    public static void closeAllDBConnections() {
        InvertedIndex.closeConnection();
        Metadata.closeConnection();
        PageIdToData.closeConnection();
        URLToPageId.closeConnection();
        WordToWordId.closeConnection();
    }
}
