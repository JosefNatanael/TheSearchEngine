package comp4321.group2.searchengine.utils;

import java.util.*;

public class QueryUtilities {
    public static HashSet<String> extractRandomQuery(HashSet<String> query, int limit) {
        int numToRemove = query.size() - limit;

        if (numToRemove < 1) {
            return query;
        }

        List<String> list = new ArrayList<>(query);
        Random rand = new Random();
        for (int i = 0; i < numToRemove; i++) {
            int indexToRemove = rand.nextInt(list.size());
            list.remove(indexToRemove);
        }

        return new LinkedHashSet<>(list);
    }
}
