package comp4321.group2.searchengine.utils;

import java.util.ArrayList;
import java.util.Arrays;

import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;

public final class WordUtilities {

    /**
     * Build DB Key prefix, with prefix of size Constants.prefixBytesLength bytes. Used for exact prefix indexing.
     *
     * @return a string XXXXXXXX@YYY, where XXXXXXXX is a zero padded wordId, and YYY is pageId
     */
    public static StringBuilder buildDBKeyPrefix(int prefixId) throws InvalidWordIdConversionException {
        String wordIdString = Integer.toString(prefixId);
        if (wordIdString.length() >= Constants.prefixBytesLength) {
            throw new InvalidWordIdConversionException(wordIdString);
        }
        StringBuilder sb = new StringBuilder();
        while (sb.length() < Constants.prefixBytesLength - wordIdString.length()) {
            sb.append('0');
        }
        sb.append(wordIdString);
        sb.append("@");
        return sb;
    }

    /**
     * Create a key for DB, with prefix of size Constants.prefixBytesLength bytes. Used for exact prefix indexing.
     *
     * @return DBKey of size Constants.prefixBytesLength bytes followed by @{documentId}
     */
    public static byte[] wordIdAndPageIdToDBKey(int wordId, int pageId) throws InvalidWordIdConversionException {
        String pageIdString = Integer.toString(pageId);
        StringBuilder sb = buildDBKeyPrefix(wordId);
        sb.append(pageIdString);

        return sb.toString().getBytes();
    }

    public static byte[] pageIdAndWordIdToDBKey(int pageId, int wordId) throws InvalidWordIdConversionException {
        String wordIdString = Integer.toString(wordId);
        StringBuilder sb = buildDBKeyPrefix(pageId);
        sb.append(wordIdString);

        return sb.toString().getBytes();
    }

    /**
     * @param arrList Array List of type T
     * @return tab separated string
     */
    public static <T> String arrayListToString(ArrayList<T> arrList) {
        StringBuilder sb = new StringBuilder();
        for (T s : arrList) {
            sb.append(s);
            sb.append("\t");
        }
        return sb.toString();
    }

    /**
     * @param tabDelimited tab separated string of integers
     * @return array list of type integers
     */
    public static ArrayList<Integer> stringToIntegerArrayList(String tabDelimited) {

        ArrayList<Integer> arrList = new ArrayList<>();
        if (tabDelimited.equals("")) {
            return arrList;
        }
        String[] intStrArray = tabDelimited.split("\t");
        for (String str : intStrArray) {
            arrList.add(Integer.parseInt(str));
        }
        return arrList;
    }

    /**
     * @param tabDelimited tab separated string of string
     * @return array list of type string
     */
    public static ArrayList<String> stringToStringArrayList(String tabDelimited) {
        String[] intStrArray = tabDelimited.split("\t");
        return new ArrayList<>(Arrays.asList(intStrArray));
    }

    public static String getSuffixFromKeyString(String key) {
        return key.substring(Constants.prefixBytesLength + 1);
    }
}
