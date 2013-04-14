/**
 * 2013-4-14
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
public class ParseZeroOne {

    public static void main(String[] args) {
        File input = new File("data/movielens-100k/u.data");
        File all = new File("data/movielens/all.dat");
        File test = new File("data/movielens/test.dat");
        File training = new File("data/movielens/training.dat");
        try {
            List<String> lines = FileUtils.readLines(input);
            List<String> testLines = new ArrayList<String>();
            List<String> trainingLines = new ArrayList<String>();
            List<String> allLines = new ArrayList<String>();
            Random random = new Random();
            for (String line:lines) {
                String[] ss = line.split("\\s");
                long userId = Long.valueOf(ss[0]);
                long itemId = Long.valueOf(ss[1]);
                double rate = Double.valueOf(ss[2]);
                long timestamp = Long.valueOf(ss[3]);
                if (rate >= 3.0) {
                    line = String.format("%d\t%d\t%f\t%d", userId, itemId, 1.0, timestamp);
                    if (random.nextDouble() >= 0.8) {
                        testLines.add(line);
                    } else {
                        trainingLines.add(line);
                    }
                    allLines.add(line);
                }
            }
            FileUtils.writeLines(all, allLines);
            FileUtils.writeLines(test, testLines);
            FileUtils.writeLines(training, trainingLines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
