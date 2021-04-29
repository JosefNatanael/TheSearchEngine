package comp4321.group2.searchengine.apimodels;

public class RelevantQuery {
    private String id;
    private String url;
    private String query;

    public RelevantQuery() {
    }

    public RelevantQuery(String id, String url, String query) {
        this.id = id;
        this.url = url;
        this.query = query;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
