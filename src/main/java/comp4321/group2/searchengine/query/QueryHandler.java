package comp4321.group2.searchengine.query;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.repositories.WordToWordId;
import comp4321.group2.searchengine.utils.StopStem;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class QueryHandler {

    String rawQuery;
    ArrayList<String> stemmedQuery = new ArrayList<String>();

    HashMap<Integer, Double> extBoolSimMap = new HashMap<Integer, Double>();
    HashMap<Integer, Double> cosSimMap = new HashMap<Integer, Double>();
    HashMap<Integer, HashMap<Integer, Double>> pageWordWeights = new HashMap<Integer, HashMap<Integer, Double>>();

    private static File stopwordsPath = new File("./src/main/resources/stopwords.txt");
    private static StopStem stopStem = new StopStem(stopwordsPath.getAbsolutePath());

    QueryHandler(String query) {
        this.rawQuery = query;

        String[] words = query.split(" ");
        for (String word: words) {
            word = word.replaceAll("\\d", "");
            String stemmedWord = stopStem.stem(word);
            if (stopStem.isStopWord(word) || stemmedWord.equals("")) {
                continue;
            }
            stemmedQuery.add(stemmedWord);
        }
    }

    /**
     *
     * @throws RocksDBException
     * @throws InvalidWordIdConversionException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void handle() throws RocksDBException, InvalidWordIdConversionException, IOException, ClassNotFoundException {
        ArrayList<Integer> queryWordIds = new ArrayList<Integer>();
        HashSet<Integer> pageIdsSet = new HashSet<Integer>();

        // Find Query Word IDs and unique Page IDs
        for (String word : stemmedQuery) {
            int wordId = WordToWordId.getValue(word);
            if (wordId == -1) {
                continue;
            }
            ArrayList<Integer> pageIds = RocksDBApi.getPageIdsOfWord(word);
            queryWordIds.add(wordId);
            pageIdsSet.addAll(pageIds);
        }

        ArrayList<Integer> pageIds = new ArrayList<Integer>(pageIdsSet);
        calculateRank(queryWordIds, pageIds);
    }

    public void calculateRank(ArrayList<Integer> queryWordIds, ArrayList<Integer> pageIds) throws RocksDBException, IOException, ClassNotFoundException, InvalidWordIdConversionException {

        ExecutorService executor = Executors.newFixedThreadPool(Constants.numCrawlerThreads);
        ArrayList<ImmutablePair<Future, QueryRunnable>> spawnedThreads = new ArrayList<>();
        Semaphore semaphore = new Semaphore(1);

        for (int i = 0; i < Constants.numCrawlerThreads; ++i) {
            QueryRunnable r = new QueryRunnable(queryWordIds, pageIds, extBoolSimMap, cosSimMap, semaphore, i);
            Future f = executor.submit(r);
            ImmutablePair<Future, QueryRunnable> pair = new ImmutablePair<>(f, r);
            spawnedThreads.add(pair);
        }
        executor.shutdown();

        try {
            executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.out.println("Failed to terminate threads");
        }



//        for (int pageId : pageIds) {
//            HashMap<Integer, Double> wordWeights = RocksDBApi.getPageWordWeights(pageId);
//            double pageLen = RocksDBApi.getPageLength(pageId);
//
//            double extBoolSim = 0.0;
//            double cosSim = 0.0;
//
//            for (int queryWordId : queryWordIds) {
//                double weight = wordWeights.get(queryWordId);
//
//                extBoolSim += Math.pow(weight, 2);
//                cosSim += weight;
//            }
//
//            extBoolSim /= queryWordIds.size();
//            extBoolSim = Math.sqrt(extBoolSim);
//            extBoolSimMap.put(pageId, extBoolSim);
//
//            cosSim /= (Math.sqrt(queryWordIds.size()) * pageLen);
//            cosSimMap.put(pageId, cosSim);
//
//            pageWordWeights.put(pageId, wordWeights);
//        }
    }
}
