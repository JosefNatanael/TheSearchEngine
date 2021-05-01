package comp4321.group2.searchengine.apimodels;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class QueryResults {


    public QueryResults() {
    }

    private String id;
    private double score;
    private String pageTitle;
    private String url;
    private String lastModifiedDate;
    private int pageSize;
    private Map<String, Integer> keywordToFreq;
    private ArrayList<String> parentUrls;
    private ArrayList<String> childUrls;

    public QueryResults(
        String id
        , double score
        , String pageTitle
        , String url
        , String lastModifiedDate
        , int pageSize
        , Map<String
        , Integer> keywordToFreq
        , ArrayList<String> parentUrls
        , ArrayList<String> childUrls
    ) {
        this.id = id;
        this.score = score;
        this.pageTitle = pageTitle;
        this.url = url;
        this.lastModifiedDate = lastModifiedDate;
        this.pageSize = pageSize;
        this.keywordToFreq = keywordToFreq;
        this.parentUrls = parentUrls;
        this.childUrls = childUrls;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public void setPageTitle(String pageTitle) {
        this.pageTitle = pageTitle;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(String lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public Map<String, Integer> getKeywordToFreq() {
        return keywordToFreq;
    }

    public void setKeywordToFreq(Map<String, Integer> keywordToFreq) {
        this.keywordToFreq = keywordToFreq;
    }

    public ArrayList<String> getParentUrls() {
        return parentUrls;
    }

    public void setParentUrls(ArrayList<String> parentUrls) {
        this.parentUrls = parentUrls;
    }

    public ArrayList<String> getChildUrls() {
        return childUrls;
    }

    public void setChildUrls(ArrayList<String> childUrls) {
        this.childUrls = childUrls;
    }
}
