package comp4321.group2.searchengine.apimodels;

import java.util.List;

public class IrrelevantQuery {

    private List<String> urls;
    private String query;

    public IrrelevantQuery() {
    }

    public IrrelevantQuery(String query, List<String> urls) {
        this.query = query;
        this.urls = urls;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<String> getUrls() {
        return urls;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }
}
