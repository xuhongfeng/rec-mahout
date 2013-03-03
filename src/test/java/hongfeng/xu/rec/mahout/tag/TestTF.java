/**
 * 2013-3-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataLine;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.Iterator;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestTF {
    private DeliciousDataModel dataModel;
    private TF tf;
    
    @BeforeClass
    public void setUp() {
        try {
            dataModel = new DeliciousDataModel(DeliciousDataConfig.RAW_DATA_FILE);
            tf = new TF(dataModel);
        } catch (IOException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void test() {
        try {
            long bookmarkId = getMostPopularBookmark();
            L.i(this, "bookmarkId = " + bookmarkId);
            PreferenceArray prefs = dataModel.getBookmarkTagPrefArray(bookmarkId);
            for (Preference pref:prefs) {
                double value = tf.getTf(bookmarkId, pref.getItemID());
                L.i(this, "tagId=" + pref.getItemID() + "\ttf="+value);
            }
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
    
    private long getMostPopularBookmark() throws TasteException {
        long id = 0;
        int max = 0;
        LongPrimitiveIterator it = dataModel.getBookmarkIds();
        while (it.hasNext()) {
            long bookmarkId = it.nextLong();
            int count = dataModel.getNumUsersWithPreferenceFor(bookmarkId);
            if (count > max) {
                id = bookmarkId;
                max = count;
                L.i(this, String.format("bookmarkId=%d\tmax=%d", bookmarkId, max));
            }
        }
        Iterator<RawDataLine> lineIterator = dataModel.getRawDataSet().lineIterator();
        while (lineIterator.hasNext()) {
            RawDataLine line = lineIterator.next();
            if (line.bookmarkId == id) {
                L.i(this, line.toString());
            }
        }
        return id;
    }
}
