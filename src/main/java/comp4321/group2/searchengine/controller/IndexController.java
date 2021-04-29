package comp4321.group2.searchengine.controller;

import comp4321.group2.searchengine.apimodels.Query;
import comp4321.group2.searchengine.apimodels.QueryResults;
import comp4321.group2.searchengine.apimodels.RelevantQuery;
import comp4321.group2.searchengine.service.QueryService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.inject.Inject;
import java.util.List;


@Controller
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
    public List<QueryResults> retrieveResults(@RequestBody Query query) {
        return queryService.retrieveResults(query);
    }

    @RequestMapping("/indexed")
    public List<String> getStemmedKeywords() {
        return queryService.getStemmedKeywords();
    }

    @RequestMapping(method = RequestMethod.POST, value = "/relevance")
    public void postRelevance(@RequestBody RelevantQuery relevantQuery) {
        queryService.postRelevance(relevantQuery);
    }

}

