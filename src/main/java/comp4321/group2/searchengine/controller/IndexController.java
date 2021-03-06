package comp4321.group2.searchengine.controller;

import comp4321.group2.searchengine.apimodels.IrrelevantQuery;
import comp4321.group2.searchengine.apimodels.Query;
import comp4321.group2.searchengine.apimodels.QueryResults;
import comp4321.group2.searchengine.apimodels.RelevantQuery;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.service.QueryService;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;


@RestController
public class IndexController {

    private final QueryService queryService;

    @Inject
    public IndexController(QueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @RequestMapping(method = RequestMethod.GET, value = "/search")
    public List<QueryResults> retrieveResults(@RequestParam String id, @RequestParam String queryString) throws RocksDBException, InvalidWordIdConversionException {
        Query query = new Query(id, queryString);
        return queryService.retrieveResults(query);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/indexed")
    public List<String> getStemmedKeywords() {
        return queryService.getStemmedKeywords();
    }

    @RequestMapping(method = RequestMethod.GET, value = "/metadata")
    public HashMap<String, Integer> getMetadata() {
        return queryService.getMetadata();
    }

    @RequestMapping(method = RequestMethod.POST, value = "/relevance")
    public void postRelevance(@RequestBody RelevantQuery relevantQuery) throws RocksDBException {
        queryService.postRelevance(relevantQuery);
    }

    @RequestMapping(method = RequestMethod.POST, value = "/irrelevance")
    public void postRelevance(@RequestBody IrrelevantQuery irrelevantQuery) throws RocksDBException {
        queryService.postIrrelevance(irrelevantQuery);
    }
}
