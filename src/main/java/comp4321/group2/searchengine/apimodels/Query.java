package comp4321.group2.searchengine.apimodels;

public class Query {
    private String id;
    private String queryString;

    public Query() {
    }

    public Query(String id, String queryString) {
        this.id = id;
        this.queryString = queryString;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }
}
