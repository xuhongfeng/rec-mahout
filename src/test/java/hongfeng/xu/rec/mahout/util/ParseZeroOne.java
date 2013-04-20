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
//        File input = new File("data/movielens-100k/u.data");
//        File all = new File("data/movielens/all.dat");
//        File test = new File("data/movielens/test.dat");
//        File training = new File("data/movielens/training.dat");
        File input = new File("data/appchina/u.dat");
        File all = new File("data/appchina/all.dat");
        File test = new File("data/appchina/test.dat");
        File training = new File("data/appchina/training.dat");
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
                if (rate >= 2.5) {
                    line = String.format("%d\t%d\t%f", userId, itemId, 1.0);
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
