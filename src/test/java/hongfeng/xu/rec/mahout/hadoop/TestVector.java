/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestVector {

    @Test
    public void testCosinSim() {
        Vector v1 = new RandomAccessSparseVector(2);
        Vector v2 = new RandomAccessSparseVector(2);
        v1.setQuick(0, 4);
        v1.setQuick(1, 3);
        v2.setQuick(0, 3);
        v2.setQuick(1, 4);
        Assert.assertEquals(HadoopHelper.cosineSimilarity(v1, v2), 24.0/25.0);
    }
    
    @Test
    public void testIntersect() {
        Vector v1 = new RandomAccessSparseVector(10);
        Vector v2 = new RandomAccessSparseVector(10);
        v1.setQuick(3, 3);
        v1.setQuick(4, 3);
        v2.setQuick(4, 3);
        v2.setQuick(5, 3);
        
        Assert.assertEquals(HadoopHelper.intersect(v1, v2), 1.0);
    }
}
