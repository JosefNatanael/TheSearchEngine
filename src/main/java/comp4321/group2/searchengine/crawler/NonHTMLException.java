package comp4321.group2.searchengine.crawler;

/** This is customized exception for those pages that have been visited before.
 */
public class NonHTMLException extends RuntimeException {

    public NonHTMLException() {
        super();
    }
}
