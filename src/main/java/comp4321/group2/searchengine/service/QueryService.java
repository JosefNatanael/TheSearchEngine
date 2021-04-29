package comp4321.group2.searchengine.service;

import comp4321.group2.searchengine.apimodels.Query;
import comp4321.group2.searchengine.apimodels.QueryResults;
import comp4321.group2.searchengine.apimodels.RelevantQuery;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class QueryService {

    public List<QueryResults> retrieveResults(Query query) {
        // TODO
        List<QueryResults> queryResults = new ArrayList<>();
        return queryResults;
    }

    public List<String> getStemmedKeywords() {
        List<String> keywords = new ArrayList<>();
        return keywords;
    }

    public void postRelevance(RelevantQuery relevantQuery) {

    }
}
