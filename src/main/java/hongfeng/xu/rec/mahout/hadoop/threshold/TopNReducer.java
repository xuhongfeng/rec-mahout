/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendedItem;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class TopNReducer extends Reducer<TypeAndNWritable, RecommendedItemsAndUserIdWritable,
    TypeAndNWritable, DoubleWritable> {
    
    private HitSet hitSet;
    private PopularityMap popularityMap;
    private UserValueMap userValueMap;
    private int itemCount;
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        hitSet = HitSet.create(context.getConfiguration());
        popularityMap = PopularityMap.create(context.getConfiguration());
        userValueMap = UserValueMap.create(context.getConfiguration());
        itemCount = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), context.getConfiguration());
    }

    public TopNReducer() {
        super();
    }
    
    @Override
    protected void reduce(TypeAndNWritable key,
            Iterable<RecommendedItemsAndUserIdWritable> values, Context context)
            throws IOException, InterruptedException {
        if (key.getType() == TypeAndNWritable.TYPE_COVERAGE) {
            Set<Integer> idSet = getRecommendedItemIds(values);
            double coverage = (double)idSet.size()/itemCount;
            context.write(key, new DoubleWritable(coverage));
        } else if (key.getType() == TypeAndNWritable.TYPE_PRECISION) {
            int hitCount = 0;
            int total = 0;
            Iterator<RecommendedItemsAndUserIdWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                RecommendedItemsAndUserIdWritable v = iterator.next();
                total += v.getItems().size();
                int userId = v.getUserId();
                for (RecommendedItem item:v.getItems()) {
                    int itemId = item.getId();
                    if (hitSet.isHit(userId, itemId)) {
                        hitCount++;
                    }
                }
            }
            double precision = (double)hitCount/total;
            context.write(key, new DoubleWritable(precision));
        } else if (key.getType() == TypeAndNWritable.TYPE_RECALL) {
            int hitCount = 0;
            int total = 0;
            Iterator<RecommendedItemsAndUserIdWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                RecommendedItemsAndUserIdWritable v = iterator.next();
                int userId = v.getUserId();
                total += userValueMap.getValue(userId);
                for (RecommendedItem item:v.getItems()) {
                    int itemId = item.getId();
                    if (hitSet.isHit(userId, itemId)) {
                        hitCount++;
                    }
                }
            }
            double recall = (double)hitCount/total;
            context.write(key, new DoubleWritable(recall));
        } else if (key.getType() == TypeAndNWritable.TYPE_POPULARITY) {
            int total = 0;
            int p = 0;
            Iterator<RecommendedItemsAndUserIdWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                RecommendedItemsAndUserIdWritable v = iterator.next();
                total += v.getItems().size();
                for (RecommendedItem item:v.getItems()) {
                    int itemId = item.getId();
                    p += Math.log(popularityMap.getPopularity(itemId) + 1);
                }
            }
            double popularity = (double)p/total;
            context.write(key, new DoubleWritable(popularity));
        }
    }
        
    private Set<Integer> getRecommendedItemIds(Iterable<RecommendedItemsAndUserIdWritable> values) {
        Set<Integer> set = new HashSet<Integer>();
        for (RecommendedItemsAndUserIdWritable v:values) {
            for (RecommendedItem item:v.getItems()) {
                set.add(item.getId());
            }
        }
        return set;
    }
}
