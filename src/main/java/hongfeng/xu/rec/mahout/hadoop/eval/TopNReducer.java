/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class TopNReducer extends Reducer<IntWritable, RecommendedItemsAndUserIdWritable,
    IntWritable, DoubleWritable> {

    public TopNReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key,
            Iterable<RecommendedItemsAndUserIdWritable> values, Context context)
            throws IOException, InterruptedException {
        int n = key.get();
        FastIDSet idSet = new FastIDSet();
        for (RecommendedItemsAndUserIdWritable v:values) {
            for (RecommendedItem item:v.getItems()) {
                idSet.add(item.getItemID());
            }
        }
        int totalCount = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(),
                context.getConfiguration());
        double coverage = (double)idSet.size()/totalCount;
        context.write(new IntWritable(n), new DoubleWritable(coverage));
    }
}
