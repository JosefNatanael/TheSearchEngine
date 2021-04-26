package comp4321.group2.searchengine.query;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.common.Constants;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.repositories.PageIdToLength;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class QueryRunnable implements Runnable {
    private final List<Integer> queryWordIds;
    private final List<Integer> pageIds;

    private final Map<Integer, Double> extBoolSimMap;
    private final Map<Integer, Double> cosSimMap;

    private final int threadNo;

    public QueryRunnable(List<Integer> queryWordIds, List<Integer> pageIds, Map<Integer, Double> extBoolSimMap, Map<Integer, Double> cosSimMap, int threadNo) {
        this.queryWordIds = queryWordIds;
        this.pageIds = pageIds;
        this.extBoolSimMap = extBoolSimMap;
        this.cosSimMap = cosSimMap;
        this.threadNo = threadNo;
    }

    @Override
    public void run() {
        int maxPagesPerThread = pageIds.size() / Constants.numCrawlerThreads + 1;

        for (int i = 0; i < maxPagesPerThread; i++) {
            try {
                int index = (i * Constants.numCrawlerThreads) + threadNo;

                if (index >= pageIds.size()) {
                    continue;
                }
                int pageId = pageIds.get(index);

                HashMap<Integer, Double> wordWeights = RocksDBApi.getPageWordWeights(pageId);
                double pageLen = PageIdToLength.getValue(pageId);

                double extBoolSim = 0.0;
                double cosSim = 0.0;

                for (int queryWordId : queryWordIds) {
                    double weight = wordWeights.get(queryWordId);

                    extBoolSim += Math.pow(weight, 2);
                    cosSim += weight;
                }

                extBoolSim /= queryWordIds.size();
                extBoolSim = Math.sqrt(extBoolSim);

                cosSim /= (Math.sqrt(queryWordIds.size()) * pageLen);

                extBoolSimMap.put(pageId, extBoolSim);
                cosSimMap.put(pageId, cosSim);

            } catch (IOException e) {
                System.out.println("IOException caught");
            } catch (RocksDBException e) {
                System.out.println("RocksDBException caught");
            } catch (InvalidWordIdConversionException e) {
                System.out.println("InvalidWordIdConversionException caught");
            } catch (ClassNotFoundException e) {
                System.out.println("ClassNotFoundException caught");
            } catch (Exception e) {
                System.out.println(e + " caught");
            }
        }
    }
}
