/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendedItem;
import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendedItemList;
import hongfeng.xu.rec.mahout.structure.RecommendedItemsAndUserIdWritable;
import hongfeng.xu.rec.mahout.structure.TypeAndNWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public class TopNMapper extends Mapper<IntWritable, RecommendedItemList,
    TypeAndNWritable, RecommendedItemsAndUserIdWritable> {

    public TopNMapper() {
        super();
    }
    
    @Override
    protected void map(IntWritable key, RecommendedItemList value,
            Context context)
            throws IOException, InterruptedException {
        List<RecommendedItem> totalList = value.getItems();
        for (int n=10; n<=DataSetConfig.TOP_N; n+=10) {
            List<RecommendedItem> list = new ArrayList<RecommendedItem>();
            for (int i=0; i<n; i++) {
                list.add(totalList.get(i));
            }
            RecommendedItemsAndUserIdWritable outValue = new RecommendedItemsAndUserIdWritable(key.get(), list);
            TypeAndNWritable mapKey = new TypeAndNWritable(TypeAndNWritable.TYPE_COVERAGE, n);
            context.write(mapKey, outValue);
            mapKey.set(TypeAndNWritable.TYPE_POPULARITY);
            context.write(mapKey, outValue);
            mapKey.set(TypeAndNWritable.TYPE_RECALL);
            context.write(mapKey, outValue);
            mapKey.set(TypeAndNWritable.TYPE_PRECISION);
            context.write(mapKey, outValue);
        }
    }

}
