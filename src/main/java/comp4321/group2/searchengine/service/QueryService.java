package comp4321.group2.searchengine.service;

import comp4321.group2.searchengine.apimodels.Query;
import comp4321.group2.searchengine.apimodels.QueryResults;
import comp4321.group2.searchengine.apimodels.RelevantQuery;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.query.QueryHandler;
import comp4321.group2.searchengine.repositories.URLToPageId;
import comp4321.group2.searchengine.repositories.WeightIndex;
import comp4321.group2.searchengine.repositories.WordToWordId;
import comp4321.group2.searchengine.utils.ByteIntUtilities;
import comp4321.group2.searchengine.utils.StopStem;
import comp4321.group2.searchengine.utils.WordUtilities;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class QueryService {

    public List<QueryResults> retrieveResults(Query query) throws RocksDBException, InvalidWordIdConversionException {
        List<QueryResults> queryResults = new ArrayList<>();

        QueryHandler qh = new QueryHandler(query.getQueryString());
        qh.handle();

        return queryResults;
    }

    public List<String> getStemmedKeywords() {
        Set<String> setOfKeywords = WordToWordId.getAll().keySet();
        return new ArrayList<>(setOfKeywords);
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
}
