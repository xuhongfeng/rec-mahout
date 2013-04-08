/**
 * 2013-4-7
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;

/**
 * @author xuhongfeng
 *
 */
public class ParseMovielens1m {
    
    public static void main(String[] args) {
        try {
            List<String> lines = FileUtils.readLines(new File("data/movielens-1m/ratings.dat"));
            List<String> trainingLines = new ArrayList<String>();
            List<String> testLines = new ArrayList<String>();
            List<String> allLines = new ArrayList<String>();
            Random random = new Random();
            for (String line:lines) {
                line = line.replace(",", "\t");
                if (random.nextDouble() > 0.9) {
                    testLines.add(line);
                } else {
                    trainingLines.add(line);
                }
                allLines.add(line);
            }
            FileUtils.writeLines(new File("data/movielens-1m/training.dat"), trainingLines);
            FileUtils.writeLines(new File("data/movielens-1m/test.dat"), testLines);
            FileUtils.writeLines(new File("data/movielens-1m/all.dat"), allLines);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
