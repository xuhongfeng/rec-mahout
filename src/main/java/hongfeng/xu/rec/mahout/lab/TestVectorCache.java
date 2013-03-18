/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.lab;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;
import hongfeng.xu.rec.mahout.hadoop.misc.BaseIndexMap.IndexType;
import hongfeng.xu.rec.mahout.hadoop.misc.IndexIdMap;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class TestVectorCache extends AbstractJob {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new TestVectorCache(), new String[] {});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int numItems = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf());
        HadoopHelper.log(this, "numItems = " + numItems);
        
        VectorCache cache = VectorCache.create(numItems, DeliciousDataConfig.getItemTagVectorPath(), getConf());
        IndexIdMap indexMap = IndexIdMap.create(IndexType.ItemIndex, getConf());
        for (int i=0; i<numItems; i++) {
            Vector vector = cache.get(i);
            if (vector == null) {
                System.out.println("vector == null, when i=" + i);
                long itemId = indexMap.get(i);
                System.out.println("itemId = " + itemId);
            }
        }
        
        return 0;
    }
}
