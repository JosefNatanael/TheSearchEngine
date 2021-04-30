package comp4321.group2.searchengine.query;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.models.Page;
import comp4321.group2.searchengine.precompute.PageRankCompute;
import comp4321.group2.searchengine.repositories.WordToWordId;
import comp4321.group2.searchengine.utils.MapUtilities;
import comp4321.group2.searchengine.utils.QueryUtilities;
import comp4321.group2.searchengine.utils.StopStem;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.*;

public class QueryHandler {

    private enum Key {
        TITLE,
        CONTENT,
    }

    final String rawQuery;
    private HashSet<String> stemmedQuery = new HashSet<>();
    private HashSet<String> unstemmedQuery = new HashSet<>();

    final Map<Integer, Double> extBoolSimMap = new ConcurrentHashMap<>();
    final Map<Integer, Double> cosSimMap = new ConcurrentHashMap<>();

    final Map<Integer, Double> adjPointsMap = new HashMap<>();
    final Map<Integer, Double> titleAdjPointsMap = new HashMap<>();

    HashMap<Integer, Double> prScoresMap = new HashMap<>();
    Map<Integer, HashMap<Integer, Double>> pageWordWeights = new HashMap<>();

    public QueryHandler(String query) {
        this.rawQuery = query;

        MutablePair<ArrayList<String>, ArrayList<String>> pair = StopStem.getStopUnstemStemPair(query.replaceAll("[^a-zA-Z ]", "").toLowerCase());
        unstemmedQuery = new LinkedHashSet<>(pair.getLeft());
        stemmedQuery = new LinkedHashSet<>(pair.getRight());

        stemmedQuery = QueryUtilities.extractRandomQuery(stemmedQuery, 20);
        unstemmedQuery = QueryUtilities.extractRandomQuery(unstemmedQuery, 20);
    }

    /**
     *
     */
    public Map<Integer, Double> handle() throws RocksDBException, InvalidWordIdConversionException {
        ArrayList<Integer> queryWordIds = new ArrayList<>();
        HashSet<Integer> pageIdsSet = new HashSet<>();
        Map<Integer, Double> totalScores = new HashMap<>();

        printQueries();

        if (stemmedQuery.isEmpty()) return totalScores;

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

        if (pageIdsSet.isEmpty()) return totalScores;

        // Init maps
        for (int pageId : pageIdsSet) {
            extBoolSimMap.put(pageId, 0.0);
            cosSimMap.put(pageId, 0.0);
            adjPointsMap.put(pageId, 0.0);
            titleAdjPointsMap.put(pageId, 0.0);
        }

        ArrayList<Integer> pageIds = new ArrayList<>(pageIdsSet);
        calculateVSM(queryWordIds, pageIds);
        calculateAdjPoints(adjPointsMap, stemmedQuery, pageIds, Key.CONTENT);
        calculateAdjPoints(titleAdjPointsMap, unstemmedQuery, pageIds, Key.TITLE);

        prScoresMap = PageRankCompute.readRankFile("pr-scores.ser");

        double maxPrScore = MapUtilities.maxUsingStreamAndMethodReference(prScoresMap);
        prScoresMap.replaceAll((k, v) -> v / maxPrScore);

        // Calculate total
        for (int pageId : pageIds) {
            totalScores.put(pageId, 0.2 * extBoolSimMap.get(pageId) + 0.2 * cosSimMap.get(pageId) + 0.2 * adjPointsMap.get(pageId) + 0.5 * titleAdjPointsMap.get(pageId) + 0.2 * prScoresMap.get(pageId));
        }

        totalScores = MapUtilities.sortByValue(totalScores, false, 50);
        printTotalScores(totalScores);

        return totalScores;
    }

    private ArrayList<ImmutablePair<Integer, Integer>> initWordStreakLocsArray(ArrayList<Integer> wordLocs) {
        ArrayList<ImmutablePair<Integer, Integer>> wordStreakLocs = new ArrayList<>();
        wordLocs.forEach((loc) -> {
            ImmutablePair<Integer, Integer> pair = new ImmutablePair<>(loc, 0);
            wordStreakLocs.add(pair);
        });
        return wordStreakLocs;
    }

    private void calculateVSM(List<Integer> queryWordIds, List<Integer> pageIds) {

        ExecutorService executor = Executors.newFixedThreadPool(Constants.numCrawlerThreads);
        ArrayList<ImmutablePair<Future, QueryRunnable>> spawnedThreads = new ArrayList<>();

        for (int i = 0; i < Constants.numCrawlerThreads; ++i) {
            QueryRunnable r = new QueryRunnable(queryWordIds, pageIds, extBoolSimMap, cosSimMap, i);
            Future<?> f = executor.submit(r);
            ImmutablePair<Future, QueryRunnable> pair = new ImmutablePair<>(f, r);
            spawnedThreads.add(pair);
        }
        executor.shutdown();

        try {
            executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.out.println("Failed to terminate threads");
        }
    }

    private void calculateAdjPoints(Map<Integer, Double> map, HashSet<String> query, List<Integer> pageIds, Key key) throws InvalidWordIdConversionException, RocksDBException {

        // Implement AdjPoints
        HashMap<Integer, ArrayList<ImmutablePair<Integer, Integer>>> currWordLocsMap = new HashMap<>();

        for (String word : query) {
            HashMap<Integer, ArrayList<Integer>> nextWordLocsMap = (key == Key.TITLE) ? RocksDBApi.getTitleWordValues(word) : RocksDBApi.getWordValues(word);

            if (nextWordLocsMap == null) continue;

            for (int pageId : pageIds) {
                if (nextWordLocsMap.containsKey(pageId)) {
                    ArrayList<Integer> nextWordLocs = nextWordLocsMap.get(pageId);
                    ArrayList<ImmutablePair<Integer, Integer>> newWordStreakLocs = new ArrayList<>();

                    int currMaxStreak = 0;
                    if (currWordLocsMap.containsKey(pageId)) {
                        int left = 0;
                        int right = 0;

                        ArrayList<ImmutablePair<Integer, Integer>> currWordStreakLocs = currWordLocsMap.get(pageId);
                        while (left < currWordStreakLocs.size() && right < nextWordLocs.size()) {
                            ImmutablePair<Integer, Integer> newPair;
                            if (currWordStreakLocs.get(left).left + 1 == nextWordLocs.get(right)) {
                                newPair = new ImmutablePair<>(nextWordLocs.get(right), currWordStreakLocs.get(left).right + 1);
                                newWordStreakLocs.add(newPair);
                                currMaxStreak = Math.max(currMaxStreak, currWordStreakLocs.get(left).right + 1);
                                left++;
                                right++;
                            } else if (nextWordLocs.get(right) < currWordStreakLocs.get(left).left) {
                                newPair = new ImmutablePair<>(nextWordLocs.get(right), 0);
                                newWordStreakLocs.add(newPair);
                                right++;
                            } else {
                                left++;
                            }
                        }

                        // Add remaining values of word locs
                        for (int i = right; i < nextWordLocs.size(); i++) {
                            ImmutablePair<Integer, Integer> newPair = new ImmutablePair<>(nextWordLocs.get(i), 0);
                            newWordStreakLocs.add(newPair);
                        }

                        currWordLocsMap.put(pageId, newWordStreakLocs);
                    } else {
                        ArrayList<ImmutablePair<Integer, Integer>> wordStreakLocs = initWordStreakLocsArray(nextWordLocs);
                        currWordLocsMap.put(pageId, wordStreakLocs);
                    }

                    double currPoints = map.get(pageId);
                    map.put(pageId, currPoints + Math.pow(currMaxStreak + 1, 2));
                } else {
                    currWordLocsMap.put(pageId, new ArrayList<>());
                }
            }
        }

        double maxAdjPoints = MapUtilities.maxUsingStreamAndMethodReference(map);
        if (maxAdjPoints != 0.0) {
            map.replaceAll((k, v) -> v / maxAdjPoints);
        }
    }

    private void printRanks() {
        prScoresMap.forEach((k, v) -> System.out.println("PR " + k + " -> " + v));

        extBoolSimMap.forEach((k, v) -> System.out.println("EXTBOOL " + k + " -> " + v));

        cosSimMap.forEach((k, v) -> System.out.println("COSSIM " + k + " -> " + v));

        adjPointsMap.forEach((k, v) -> System.out.println("ADJ " + k + " -> " + v));

        titleAdjPointsMap.forEach((k, v) -> System.out.println("ADJ TITLE " + k + " -> " + v));
    }

    private void printTotalScores(Map<Integer, Double> map) {
        map.forEach((k, v) -> {
            try {
                Page pageData = RocksDBApi.getPageData(k);
                System.out.println(pageData.getUrl() + "\t" + v);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void printQueries() {
        System.out.println("\nStemmed query :");
        for (String query : stemmedQuery) {
            System.out.println(query);
        }

        System.out.println("\nUnstemmed query :");
        for (String query : unstemmedQuery) {
            System.out.println(query);
        }
        System.out.println();
    }
}
