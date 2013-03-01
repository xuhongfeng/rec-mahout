/**
 * 2013-3-1
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.model;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataLine;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataLineArray;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataSet;
import hongfeng.xu.rec.mahout.util.DataModelUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.common.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestDeliciousDataModel {
    private static File RAW_DATA_FILE = DeliciousDataConfig.RAW_DATA_FILE;
    private DeliciousDataModel dataModel;
    private List<String> lines;
    private Random random;
    
    @BeforeClass
    public void setUp() {
        try {
            dataModel = new DeliciousDataModel(RAW_DATA_FILE);
            lines = FileUtils.readLines(RAW_DATA_FILE);
            random = new Random();
        } catch (IOException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void testRawDataSet() {
        RawDataSet rawDataSet = dataModel.getRawDataSet();
        Assert.assertEquals(rawDataSet.lineCount(), lines.size());
        for (int i=0; i<100; i++) {
            int k = random.nextInt(lines.size());
            String line = lines.get(k);
            RawDataLine rawDataLine = RawDataLine.parse(line);
            RawDataLineArray array = rawDataSet.get(rawDataLine.userId);
            boolean found = false;
            for (RawDataLine l:array) {
                if (l.equals(rawDataLine)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }
    }
    
    @Test
    public void testDataModel() {
        int k = random.nextInt(lines.size());
        String line = lines.get(k);
        RawDataLine rawDataLine = RawDataLine.parse(line);
        int countUB = 0;
        int countUT = 0;
        int countBT = 0;
        for (String l:lines) {
            RawDataLine otherLine = RawDataLine.parse(l);
            if (rawDataLine.userId==otherLine.userId && rawDataLine.bookmarkId==otherLine.bookmarkId) {
                countUB++;
            }
            if (rawDataLine.userId==otherLine.userId && rawDataLine.tagId==otherLine.tagId) {
                countUT++;
            }
            if (rawDataLine.bookmarkId==otherLine.bookmarkId && rawDataLine.tagId==otherLine.tagId) {
                countBT++;
            }
        }
        try {
            Assert.assertEquals(dataModel.getPreferenceValue(rawDataLine.userId,
                    rawDataLine.bookmarkId), (float)countUB);
            Assert.assertEquals(dataModel.getUserTagValue(rawDataLine.userId,
                    rawDataLine.tagId), (float)countUT);
            Assert.assertEquals(dataModel.getBookmarkTagValue(rawDataLine.bookmarkId,
                    rawDataLine.tagId), (float)countBT);
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void testSplit() {
        try {
            Pair<DeliciousDataModel, DeliciousDataModel> pair = DataModelUtils.splitDeliciousDataModel(dataModel
                    , 0.5, 0.8);
            DeliciousDataModel trainingDataModel = pair.getFirst();
            DeliciousDataModel testDataModel = pair.getSecond();
            
            RawDataSet trainingDataSet = trainingDataModel.getRawDataSet();
            RawDataSet testDataSet = testDataModel.getRawDataSet();
            
            double actualTrainingPercentage = ((double)trainingDataSet.lineCount())/dataModel.getRawDataSet().lineCount();
            double actualTestPercentage = ((double)testDataSet.lineCount())/dataModel.getRawDataSet().lineCount();
            L.i(this, "actualTrainingPercentage = " + actualTrainingPercentage);
            L.i(this, "actualTestPercentage= " + actualTestPercentage);
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
}
