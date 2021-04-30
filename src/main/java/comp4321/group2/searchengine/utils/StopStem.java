package comp4321.group2.searchengine.utils;

import org.apache.commons.lang3.tuple.MutablePair;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;

public final class StopStem {

    private static final Porter porter;
    private static final java.util.HashSet<String> stopWords;

    static {
        porter = new Porter();
        stopWords = new java.util.HashSet<>();

        InputStream in = StopStem.class.getResourceAsStream("/stopwords.txt");
        Scanner pathScanner = new Scanner(in);

        while (pathScanner.hasNext()) {
            stopWords.add(pathScanner.next());
        }
        pathScanner.close();

    }

    public static boolean isStopWord(String str) {
        return stopWords.contains(str);
    }

    public static String stem(String str) {
        return str.isEmpty() ? "" : porter.stripAffixes(str);
    }

    public static MutablePair<ArrayList<String>, ArrayList<String>> getStopUnstemStemPair(String dirtyString) {

        Vector<String> words = new Vector<>();
        ArrayList<String> unstemmedQuery = new ArrayList<>();
        ArrayList<String> stemmedQuery = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(dirtyString);

        while (st.hasMoreTokens()) {
            words.add(st.nextToken());
        }

        for (String word : words) {
            unstemmedQuery.add(word);
            word = word.replaceAll("\\d", "");
            String stemmedWord = StopStem.stem(word);
            if (StopStem.isStopWord(word) || stemmedWord.equals("")) {
                continue;
            }
            stemmedQuery.add(stemmedWord);
        }

        return new MutablePair<>(unstemmedQuery, stemmedQuery);
    }
}
