package comp4321.group2.searchengine.utils;

import java.io.*;

public final class StopStem {

    private final Porter porter;
    private final java.util.HashSet<String> stopWords;

    public boolean isStopWord(String str) {
        return stopWords.contains(str);
    }

    public StopStem(String str) {
        super();
        porter = new Porter();
        stopWords = new java.util.HashSet<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(str));
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String stem(String str) {
        return str.isEmpty() ? "" : porter.stripAffixes(str);
    }
}
