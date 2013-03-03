/**
 * 2013-3-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.tag.TagAllocator.AllocateItem;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestTagAllocator {
    private DeliciousDataModel deliciousDataModel;
    private TagAllocator tagAllocator;
    @BeforeClass
    public void setUp() {
        try {
            deliciousDataModel = new DeliciousDataModel(DeliciousDataConfig.RAW_DATA_FILE);
            tagAllocator = new TagAllocator(deliciousDataModel.getBookmarkTagModel());
        } catch (IOException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void testAllocateTag() {
        try {
            List<AllocateItem> items = tagAllocator.allocateTagDesc(1631);
            double total = 0;
            for (AllocateItem item:items) {
                L.i(this, "item = " + item);
                total += item.getValue();
            }
            L.i(this, "total = "+ total);
            Assert.assertTrue(Math.abs(1-total) < 0.0001);
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void testAllocateBookmark() {
        try {
            List<AllocateItem> items = tagAllocator.allocateBookmarkDesc(993);
            double total = 0;
            for (AllocateItem item:items) {
                L.i(this, "item = " + item);
                total += item.getValue();
            }
            L.i(this, "total = "+ total);
            Assert.assertTrue(Math.abs(1-total) < 0.0001);
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
}
