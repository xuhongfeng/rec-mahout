/**
 * 2013-3-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.structure;

import hongfeng.xu.rec.mahout.util.L;
import hongfeng.xu.rec.mahout.util.TestDataGenerator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import junit.framework.Assert;

import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestFixedSizePriorityQueue {

    @Test
    public void testAccuration() {
        FixedSizePriorityQueue<RecommendedItem> queue = 
                new FixedSizePriorityQueue<RecommendedItem>(100, COMPARATOR);
        List<RecommendedItem> items = genItems(1000);
        for (RecommendedItem item:items) {
            queue.add(item);
        }
        
        Assert.assertEquals(100, queue.size());
        for (int i=0; i<100; i++) {
            float expected = (float) (900+i);
            RecommendedItem item = queue.poll();
            Assert.assertEquals(expected, item.getValue());
        }
    }
    
    @Test
    public void testPerformance() {
        FixedSizePriorityQueue<RecommendedItem> queue = 
                new FixedSizePriorityQueue<RecommendedItem>(100, COMPARATOR);
        List<RecommendedItem> items = genItems(100000);
        long start = System.currentTimeMillis();
        for (RecommendedItem item:items) {
            queue.add(item);
        }
        L.i(this, "cost = " + (System.currentTimeMillis()-start));
    }
    
    private static List<RecommendedItem> genItems(int size) {
        List<RecommendedItem> items = new ArrayList<RecommendedItem>(size);
        float[] array = TestDataGenerator.genRandomFloatArray(size);
        for (int i=0; i<size; i++) {
            RecommendedItem item = new GenericRecommendedItem(i, array[i]);
            items.add(item);
        }
        return items;
    }
    
    private static Comparator<RecommendedItem> COMPARATOR = new Comparator<RecommendedItem>() {
        
        @Override
        public int compare(RecommendedItem o1, RecommendedItem o2) {
            if (o1.getValue() < o2.getValue()) {
                return -1;
            }
            return 1;
        }
    };
}
