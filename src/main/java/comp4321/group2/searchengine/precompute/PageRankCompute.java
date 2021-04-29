package comp4321.group2.searchengine.precompute;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.models.Page;
import org.jgrapht.alg.interfaces.VertexScoringAlgorithm;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class PageRankCompute {

    DefaultDirectedWeightedGraph<Integer, DefaultEdge> linkGraph = new DefaultDirectedWeightedGraph<>(DefaultEdge.class);
    VertexScoringAlgorithm<Integer, Double> pr;
    double dampingFactor = 1.0;

    public PageRankCompute(double dampingFactor) {
        this.dampingFactor = dampingFactor;
    }

    public void compute() {
        makeGraph();
        pr = new PageRank<>(linkGraph, dampingFactor);
        writeRankFile();
    }

    private void makeGraph() {
        HashMap<Integer, Page> pageDataMap = null;
        try {
            pageDataMap = RocksDBApi.getAllPageData();
        } catch (Exception ignore) {
            System.out.println("Exception caught when getting page data");
        }

        if (pageDataMap == null) {
            return;
        }

        HashSet<Integer> pageIds = new HashSet<>(pageDataMap.keySet());

        for (Map.Entry<Integer, Page> entry : pageDataMap.entrySet()) {
            int pageId = entry.getKey();
            Page pageData = entry.getValue();

            linkGraph.addVertex(pageId);

            String[] childUrls = pageData.getChildUrls().split("\t");
            for (String url : childUrls) {
                try {
                    int childPageId = RocksDBApi.getPageIdFromURL(url);

                    if (!pageIds.contains(childPageId)) continue;

                    linkGraph.addVertex(childPageId);
                    linkGraph.addEdge(pageId, childPageId);
                } catch (Exception e) {
                    System.out.println("Exception caught when getting page id");
                }
            }
        }
    }

    public HashMap<Integer, Double> getRankMap() {
        return new HashMap<>(pr.getScores());
    }

    public static HashMap<Integer, Double> readRankFile(String path) {
        Map<Integer, Double> scores = null;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            scores = (Map<Integer, Double>) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException c) {
            System.out.println("PR Scores file not found");
            c.printStackTrace();
        }

        return new HashMap<>(scores);
    }

    private void writeRankFile() {
        Map<Integer, Double> scores = pr.getScores();
        try {
            FileOutputStream fileOut =
                new FileOutputStream("pr-scores.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(scores);
            out.close();
            fileOut.close();

            System.out.print("Serialized data is saved in /pr-scores.ser\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
