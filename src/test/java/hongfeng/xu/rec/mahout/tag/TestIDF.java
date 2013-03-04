/**
 * 2013-3-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataLine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestIDF {
    private DeliciousDataModel dataModel;
    
    @BeforeClass
    public void setUp() {
        try {
            dataModel = new DeliciousDataModel(DeliciousDataConfig.RAW_DATA_FILE);
        } catch (IOException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void test() {
        long tagId = new Random().nextInt(10000);
        Iterator<RawDataLine> it = dataModel.getRawDataSet().lineIterator();
        FastIDSet set1 = new FastIDSet();
        FastIDSet set2 = new FastIDSet();
        while (it.hasNext()) {
            RawDataLine line = it.next();
            set1.add(line.bookmarkId);
            if (line.tagId==tagId) {
                set2.add(line.bookmarkId);
            }
        }
        double d = set1.size();
        double dw = set2.size();
        double expectedIdf = Math.log(d/dw);
        double actualIdf = 0;
        try {
            actualIdf = new IDF(dataModel).getIdf(tagId);
        } catch (TasteException e) {
            Assert.fail("", e);
            return;
        }
        Assert.assertEquals(actualIdf, expectedIdf);
    }
}
