package comp4321.group2.searchengine.precompute;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.repositories.*;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class FastCompute {

    public FastCompute() {
    }

    public void processWordIdToIdfEntries() {
        //iterate each word ID, compute idf, length
        HashMap<String, Integer> wordToWordID = WordToWordId.getAll();
        HashMap<String, Integer> latestIndex = Metadata.getAll();
        int numDocs = latestIndex.get("page");

        wordToWordID.entrySet().parallelStream().forEach(pair -> {
            String word = pair.getKey();
            int wordId = pair.getValue();
            ArrayList<Integer> result = null;
            try {
                result = RocksDBApi.getPageIdsOfWord(word);
            } catch (Exception e) {
                System.out.println(e + " caught");
            }
            assert result != null;
            int df = result.size();
            double idf = (Math.log(numDocs / (double) df) / Math.log(2));

            try {
                WordIdToIdf.addEntry(wordId, idf);
            } catch (Exception e) {
                System.out.println(e + " caught");
            }
        });
    }

    public void processWeightsAndPageLength() {
        ForwardIndex.getAll().entrySet().parallelStream().forEach(pair -> {
            int pageId = pair.getKey();

            HashMap<Integer, Double> wordWeights = null;
            try {
                wordWeights = computePageWordWeights(pageId);
            } catch (Exception e) {
                System.out.println(e + " caught");
            }

            // Compute L2 length here
            double sum = 0.0;
            for (Double value : wordWeights.values()) {
                sum += value;
            }
            double length_result = Math.sqrt(sum);

            //store page length in db
            try {
                PageIdToLength.addEntry(pageId, length_result);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Store weights to WeightIndex
            HashMap<byte[], Double> wordWeightsProperKey = new HashMap<>();

            wordWeights.forEach((key, value) -> {
                int wordId = key;
                double weight = value;

                byte[] properKey = null;
                try {
                    properKey = WordUtilities.pageIdAndWordIdToDBKey(pageId, wordId);
                } catch (Exception e) {
                    System.out.println(e + " caught");
                }
                wordWeightsProperKey.put(properKey, weight);
            });

            try {
                WeightIndex.createEntriesInBatch(wordWeightsProperKey);
            } catch (Exception e) {
                System.out.println(e + " caught");
            }
        });
    }

    private HashMap<Integer, Double> computePageWordWeights(int pageId) throws RocksDBException, IOException, ClassNotFoundException, InvalidWordIdConversionException {
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

    public void computePageParents() throws IOException, ClassNotFoundException {
        HashMap<Integer, Page> pageDataMap = RocksDBApi.getAllPageData();
        HashSet<Integer> pageIds = new HashSet<>(pageDataMap.keySet());

        HashMap<Integer, HashSet<Integer>> pageIdToParentsMap = new HashMap<>();

        for (Map.Entry<Integer, Page> entry : pageDataMap.entrySet()) {
            int pageId = entry.getKey();
            Page pageData = entry.getValue();

            String[] childUrls = pageData.getChildUrls().split("\t");
            for (String url : childUrls) {
                try {
                    int childPageId = RocksDBApi.getPageIdFromURL(url);

                    if (!pageIds.contains(childPageId) || pageId == childPageId) continue;

                    if (pageIdToParentsMap.containsKey(childPageId)) {
                        pageIdToParentsMap.get(childPageId).add(pageId);
                    } else {
                        HashSet<Integer> newSet = new HashSet<>();
                        newSet.add(pageId);
                        pageIdToParentsMap.put(childPageId, newSet);
                    }
                } catch (Exception e) {
                    System.out.println("Exception caught when getting page id");
                }
            }
        }

        for (Map.Entry<Integer, HashSet<Integer>> entry : pageIdToParentsMap.entrySet()) {
            try {
                RocksDBApi.addPageParents(entry.getKey(), new ArrayList<>(entry.getValue()));
            } catch (Exception e) {
                System.out.println("Exception caught when inserting parent ids");
            }
        }
    }

    public void computePageRank() {
        PageRankCompute pr = new PageRankCompute(0.85);
        pr.compute();
    }
}
