package comp4321.group2.searchengine.service;

import comp4321.group2.searchengine.apimodels.*;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.query.QueryHandler;
import comp4321.group2.searchengine.repositories.*;
import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.MapUtilities;
import comp4321.group2.searchengine.utils.StopStem;
import comp4321.group2.searchengine.utils.WordUtilities;
import comp4321.group2.searchengine.models.Page;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.*;

@Service
public class QueryService {

    public List<QueryResults> retrieveResults(Query query) throws RocksDBException, InvalidWordIdConversionException {
        ArrayList<QueryResults> queryResults = new ArrayList<>();

        QueryHandler qh = new QueryHandler(query.getQueryString());
        HashMap<Integer, Double> pageIdToWeight = (HashMap<Integer, Double>) qh.handle();

        pageIdToWeight.forEach((pageId, score) -> {
            try {
                Page page = PageIdToData.getValue(pageId);
                String id = UUID.randomUUID().toString();
                String pageTitle = page.getTitle();
                String url = page.getUrl();
                ZonedDateTime lastModified_zoned = page.getLastModified();
                String lastModified = lastModified_zoned != null ? lastModified_zoned.toString() : "n/a";
                int pageSize = page.getSize();
                Map<String, Integer> keywordToFreq = new HashMap<>();

                ArrayList<Integer> wordIds = ForwardIndex.getValue(pageId);

                for(Integer wordId: wordIds){
                    String word = WordIdToWord.getValue(wordId);
                    int frequency = InvertedIndex.getValueByKey(WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId)).size();
                    keywordToFreq.put(word, frequency);
                }

                keywordToFreq = MapUtilities.sortByValue(keywordToFreq, false, 5);

                ArrayList<String> parentUrls = new ArrayList<>();

                ArrayList<Integer> parentIds = PageIdToParentIds.getValue(pageId);

                for(Integer parentId: parentIds){
                    parentUrls.add(PageIdToData.getValue(parentId).getUrl());
                }

                ArrayList<String> childUrls = WordUtilities.stringToStringArrayList(page.getChildUrls());

                QueryResults qs = new QueryResults(id, score, pageTitle, url, lastModified, pageSize, keywordToFreq, parentUrls, childUrls);
                queryResults.add(qs);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return queryResults;
    }

    public List<String> getStemmedKeywords() {
        Set<String> setOfKeywords = WordToWordId.getAll().keySet();
        return new ArrayList<>(setOfKeywords);
    }

    public HashMap<String, Integer> getMetadata() {
        return Metadata.getAll();
    }

    public void postRelevance(RelevantQuery relevantQuery) throws RocksDBException {
        String queryString = relevantQuery.getQuery();
        String queryUrl = relevantQuery.getUrl();
        int pageId = URLToPageId.getValue(queryUrl);

        ArrayList<String> words = StopStem.getStopUnstemStemPair(queryString).getRight();
        words.forEach(word -> {
            int wordId = 0;
            try {
                wordId = WordToWordId.getValue(word);
                byte[] weightIndexKey = WordUtilities.pageIdAndWordIdToDBKey(pageId, wordId);
                double currentWeight = WeightIndex.getValueByKey(weightIndexKey);
                WeightIndex.addEntry(weightIndexKey, ByteIntUtilities.doubleToByteArray(currentWeight * Constants.relevanceMultiplier));
            } catch (RocksDBException | InvalidWordIdConversionException e) {
                e.printStackTrace();
            }
        });
    }

    public void postIrrelevance(IrrelevantQuery irrelevantQuery) throws RocksDBException {
        String queryString = irrelevantQuery.getQuery();
        ArrayList<String> words = StopStem.getStopUnstemStemPair(queryString).getRight();

        List<String> urls = irrelevantQuery.getUrls();
        for (String url : urls) {
            int pageId = URLToPageId.getValue(url);

            words.forEach(word -> {
                int wordId = 0;
                try {
                    wordId = WordToWordId.getValue(word);
                    byte[] weightIndexKey = WordUtilities.pageIdAndWordIdToDBKey(pageId, wordId);
                    double currentWeight = WeightIndex.getValueByKey(weightIndexKey);
                    if (currentWeight != 0.0)  {
                        WeightIndex.addEntry(weightIndexKey, ByteIntUtilities.doubleToByteArray(currentWeight * Constants.irrelevanceMultiplier));
                    }
                } catch (RocksDBException | InvalidWordIdConversionException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
