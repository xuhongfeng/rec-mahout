/**
 * 2013-3-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;
import hongfeng.xu.rec.mahout.util.L;

import java.util.Comparator;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
@Deprecated
public class TestTagSimilarity {
    private ItemSimilarity similarity;
    private DeliciousDataModel totalDataModel;
    
    @BeforeClass
    public void setUp() {
        try {
            long start = System.currentTimeMillis();
            totalDataModel = new DeliciousDataModel(DeliciousDataConfig.RAW_DATA_FILE);
            long now = System.currentTimeMillis();
            L.i(this, "create data model cost : " + (now - start));
            start = now;
            
//            UncenteredCosineSimilarity uncenteredCosineSimilarity = new UncenteredCosineSimilarity(totalDataModel.getBookmarkTagModel());
//            similarity = new CachingItemSimilarity(uncenteredCosineSimilarity, totalDataModel.getBookmarkTagModel());
//            PearsonCorrelationSimilarity pearsonSimilarity= new PearsonCorrelationSimilarity(totalDataModel.getBookmarkTagModel());
//            similarity = new CachingItemSimilarity(pearsonSimilarity, totalDataModel.getBookmarkTagModel());
            TagSimilarity tagSimilarity = new TagSimilarity(totalDataModel);
            similarity = new CachingItemSimilarity(tagSimilarity, totalDataModel.getBookmarkTagModel());
            now = System.currentTimeMillis();
            now = System.currentTimeMillis();
            L.i(this, "create similarity cost : " + (now - start));
            start = now;
        } catch (Throwable e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void test() {
        try {
//            LongPrimitiveIterator tagIdIterator = totalDataModel.getTagIds();
//            long tagId = tagIdIterator.nextLong();
            long tagId = 1631;
//            FixedSizePriorityQueue<Item> queue = new FixedSizePriorityQueue<Item>(
//                    10, INCR_COMPARATOR);
//            similarity.itemSimilarity(993, 9);
            long[] otherIds = similarity.allSimilarItemIDs(tagId);
            double[] similaritys = similarity.itemSimilarities(tagId, otherIds);
            for (int i=0; i<otherIds.length; i++) {
                L.i(this, "otherTagId=" + otherIds[i] + " , similarity = " + similaritys[i]);
//                queue.add(new Item(tagId, otherIds[i], similaritys[i]));
            }
//            List<Item> items = new ArrayList<Item>();
//            items.addAll(queue);
//            Collections.sort(items, DESC_COMPARATOR);
//            for (Item item:items) {
//                L.i(this, item.toString());
//            }
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
    
    private static Comparator<Item> INCR_COMPARATOR = new Comparator<Item>() {
        
        @Override
        public int compare(Item o1, Item o2) {
            if (o1.similarity < o2.similarity) {
                return -1;
            }
            return 1;
        }
    };
    
    private static Comparator<Item> DESC_COMPARATOR = new Comparator<Item>() {
        
        @Override
        public int compare(Item o1, Item o2) {
            return -INCR_COMPARATOR.compare(o1, o2);
        }
    };
    
    private static class Item {
        public final long id1;
        public final long id2;
        public final double similarity;
        
        public Item(long id1, long id2, double similarity) {
            super();
            if (id1 < id2) {
                this.id1 = id1;
                this.id2 = id2;
            } else {
                this.id2 = id1;
                this.id1 = id2;
            }
            this.similarity = similarity;
        }

        @Override
        public String toString() {
            return "Item [id1=" + id1 + ", id2=" + id2 + ", similarity="
                    + similarity + "]";
        }
    }
}
