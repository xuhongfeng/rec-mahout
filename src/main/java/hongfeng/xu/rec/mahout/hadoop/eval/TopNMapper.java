/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;

/**
 * @author xuhongfeng
 *
 */
public class TopNMapper extends Mapper<VarLongWritable, RecommendedItemsWritable,
    IntWritable, RecommendedItemsAndUserIdWritable> {

    public TopNMapper() {
        super();
    }
    
    @Override
    protected void map(VarLongWritable key, RecommendedItemsWritable value,
            Context context)
            throws IOException, InterruptedException {
        List<RecommendedItem> totalList = value.getRecommendedItems();
        for (int n=10; n<=DeliciousDataConfig.TOP_N; n+=10) {
            List<RecommendedItem> list = new ArrayList<RecommendedItem>();
            for (int i=0; i<n; i++) {
                list.add(totalList.get(i));
            }
            context.write(new IntWritable(n), new RecommendedItemsAndUserIdWritable(key.get(), list));
        }
    }

}
