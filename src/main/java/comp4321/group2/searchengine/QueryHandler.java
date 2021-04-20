package comp4321.group2.searchengine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import org.rocksdb.RocksDBException;

public class QueryHandler {

    public static void handle(String query) throws RocksDBException, InvalidWordIdConversionException, IOException, ClassNotFoundException {
        String[] queryWords = query.split(" ");
        ArrayList<Integer> queryWordIds = new ArrayList<Integer>();
        HashSet<Integer> pageIdsSet = new HashSet<Integer>();

        for (String word : queryWords) {
            int wordId = RocksDBApi.getWordIdOfWord(word);
            if (wordId == -1) {
                continue;
            }
            ArrayList<Integer> pageIds = RocksDBApi.getPageIdsOfWord(word);
            queryWordIds.add(wordId);
            pageIdsSet.addAll(pageIds);
        }

        HashMap<Integer, HashMap<Integer, Double>> pageWordWeights = new HashMap<Integer, HashMap<Integer, Double>>();
        HashMap<Integer, Double> extBoolSimMap = new HashMap<Integer, Double>();
        HashMap<Integer, Double> cosSimMap = new HashMap<Integer, Double>();

        for (int pageId : pageIdsSet) {
            HashMap<Integer, Double> wordWeights = RocksDBApi.getPageWordWeights(pageId);
            double pageLen = RocksDBApi.getPageLength(pageId);

            double extBoolSim = 0.0;
            double cosSim = 0.0;

            for (int queryWordId : queryWordIds) {
                double weight = wordWeights.get(queryWordId);

                extBoolSim += Math.pow(weight, 2);
                cosSim += weight;
            }

            extBoolSim /= queryWordIds.size();
            extBoolSim = Math.sqrt(extBoolSim);
            extBoolSimMap.put(pageId, extBoolSim);

            cosSim /= (Math.sqrt(queryWordIds.size()) * pageLen);
            cosSimMap.put(pageId, cosSim);

            pageWordWeights.put(pageId, wordWeights);
        }
    }
}
