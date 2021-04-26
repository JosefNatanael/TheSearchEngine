package comp4321.group2.searchengine.precompute;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.repositories.*;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FastCompute {

    public FastCompute() {
    }

    public void processWordIdToIdfEntries() throws RocksDBException, InvalidWordIdConversionException {
        //iterate each word ID, compute idf, length
        HashMap<String, Integer> wordToWordID = WordToWordId.getAll();
        HashMap<String, Integer> latestIndex = Metadata.getAll();
        int numDocs = latestIndex.get("page");
        int df;
        double idf;

        for (Map.Entry<String, Integer> pair : wordToWordID.entrySet()) {
            String word = pair.getKey();
            int wordId = pair.getValue();
            ArrayList<Integer> result = RocksDBApi.getPageIdsOfWord(word);
            df = result.size();
            idf = (Math.log(numDocs / (double) df) / Math.log(2));

            WordIdToIdf.addEntry(wordId, idf);
        }
    }

    public void processPageIdToL2Length() {
        HashMap<String, Integer> URLToPageID = URLToPageId.getAll();

        URLToPageID.entrySet().parallelStream().forEach(pair -> {
            int pageId = pair.getValue();
            HashMap<Integer, Double> wordWeights = null;
            try {
                wordWeights = RocksDBApi.getPageWordWeights(pageId);
            } catch (Exception e) {
                e.printStackTrace();
            }

            double sum = 0.0;
            for (Double value : wordWeights.values()) {
                sum += value;
            }
            double length_result = Math.sqrt(sum);

            //store in db
            try {
                PageIdToLength.addEntry(pageId, length_result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) throws RocksDBException, InvalidWordIdConversionException, IOException, ClassNotFoundException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect();

        FastCompute compute = new FastCompute();

        compute.processWordIdToIdfEntries();
        compute.processPageIdToL2Length();

        RocksDBApi.closeAllDBConnections();
    }
}
