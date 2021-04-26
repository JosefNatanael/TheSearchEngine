package comp4321.group2.searchengine.precompute;

import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;
import comp4321.group2.searchengine.repositories.*;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FastCompute {

    public FastCompute() {
    }

    public void postIndexProcess() throws RocksDBException, InvalidWordIdConversionException {
        //iterate each word ID, compute idf, length
        HashMap<String, Integer> wordToWordID = WordToWordId.getAll();
        HashMap<String, Integer> latestIndex = Metadata.getAll();
        int numDocs = latestIndex.get("page");
        int df;
        double idf;

        for (Map.Entry<String, Integer> pair : wordToWordID.entrySet()) {
            String word = pair.getKey();
            int wordId = pair.getValue();
            ArrayList<Integer> result = RocksDBApi.getPageIdsOfWord(word);
            df = result.size();
            idf = (Math.log(numDocs / (double) df) / Math.log(2));

            WordIdToIdf.addEntry(wordId, idf);

        }
    }

    public void computeL2Length() throws RocksDBException, InvalidWordIdConversionException {
        HashMap<String, Integer> URLToPageID = URLToPageId.getAll();
        int wordId, tf;

        double idf, accumulator = 0.0, length_result;
        //from forward get l2 length
        for (Map.Entry<String, Integer> pair : URLToPageID.entrySet()) {
            int pageId = pair.getValue();

            //get wordIdList from forward index
            ArrayList<Integer> wordIdList = ForwardIndex.getValue(pageId);

            //for each word id
            for (int i = 0; i < wordIdList.size(); ++i) {
                wordId = wordIdList.get(i);
                //get idf of the word
                idf = WordIdToIdf.getValue(wordId);

                //get tf of the word from getvaluebykey of index 'wordID@pageID'
                tf = RocksDBApi.getInvertedValuesFromKey(wordId, pageId).size();

                //get the square of tf x idf, then accumulate
                accumulator += Math.pow(tf * idf, 2);
            }

            //square root the accumulated squared tf idf
            length_result = Math.sqrt(accumulator);

            //store in db
            PageIdToLength.addEntry(pageId, length_result);
        }
    }

    public static void main(String[] args) throws RocksDBException, InvalidWordIdConversionException {
        RocksDBApi.closeAllDBConnections();
        RocksDBApi.connect();

        FastCompute compute = new FastCompute();

        compute.postIndexProcess();
        compute.computeL2Length();

        RocksDBApi.closeAllDBConnections();
    }
}
