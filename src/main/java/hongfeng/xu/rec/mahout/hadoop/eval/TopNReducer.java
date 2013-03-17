/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
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
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        hitSet = HitSet.create(context.getConfiguration());
        popularityMap = PopularityMap.create(context.getConfiguration());
        userValueMap = UserValueMap.create(context.getConfiguration());
    }

    public TopNReducer() {
        super();
    }
    
    @Override
    protected void reduce(TypeAndNWritable key,
            Iterable<RecommendedItemsAndUserIdWritable> values, Context context)
            throws IOException, InterruptedException {
        if (key.getType() == TypeAndNWritable.TYPE_COVERAGE) {
            FastIDSet idSet = getRecommendedItemIds(values);
            int totalCount = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(),
                    context.getConfiguration());
            double coverage = (double)idSet.size()/totalCount;
            context.write(key, new DoubleWritable(coverage));
        } else if (key.getType() == TypeAndNWritable.TYPE_PRECISION) {
            int hitCount = 0;
            int total = 0;
            Iterator<RecommendedItemsAndUserIdWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                RecommendedItemsAndUserIdWritable v = iterator.next();
                total += v.getItems().size();
                long userId = v.get();
                for (RecommendedItem item:v.getItems()) {
                    long itemId = item.getItemID();
                    if (hitSet.isHit(userId, itemId)) {
                        hitCount++;
                    }
                }
            }
//            HadoopHelper.log(this, "hitCount = " + hitCount);
//            HadoopHelper.log(this, "total = " + total);
            double precision = (double)hitCount/total;
            context.write(key, new DoubleWritable(precision));
        } else if (key.getType() == TypeAndNWritable.TYPE_RECALL) {
            int hitCount = 0;
            int total = 0;
            Iterator<RecommendedItemsAndUserIdWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                RecommendedItemsAndUserIdWritable v = iterator.next();
                long userId = v.get();
                total += userValueMap.getValue(userId);
                for (RecommendedItem item:v.getItems()) {
                    long itemId = item.getItemID();
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
                    long itemId = item.getItemID();
                    p += Math.log(popularityMap.getPopularity(itemId) + 1);
                }
            }
            double popularity = (double)p/total;
            context.write(key, new DoubleWritable(popularity));
        }
    }
        
    private FastIDSet getRecommendedItemIds(Iterable<RecommendedItemsAndUserIdWritable> values) {
        FastIDSet idSet = new FastIDSet();
        for (RecommendedItemsAndUserIdWritable v:values) {
            for (RecommendedItem item:v.getItems()) {
                idSet.add(item.getItemID());
            }
        }
        return idSet;
    }
}
