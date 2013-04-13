/**
 * 2013-3-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class PopularRecommender extends BaseRecommender {
    
    @Override
    protected int innerRun() throws Exception {
        PopularityItemJob popularityItemJob = new PopularityItemJob();
        runJob(popularityItemJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getPopularItemSortPath(), true);
    
        Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class,
                MyMapper.class, IntWritable.class, RecommendedItemList.class,
                SequenceFileOutputFormat.class);
        if (!job.waitForCompletion(true)) {
            return -1;
        }
        
        return 0;
    }

    public static class MyMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, RecommendedItemList> {
        private RecommendedItemList itemList;
        
        public MyMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            PopularItemQueue queue = PopularItemQueue.create(context.getConfiguration());
            int n = DataSetConfig.TOP_N;
            List<RecommendedItem> items = new ArrayList<RecommendedItem>();
            for (int i=0; i<n; i++) {
                int id = queue.getItemId(i);
                RecommendedItem item = new RecommendedItem(id, 1.0);
                items.add(item);
            }
            itemList = new RecommendedItemList(items);
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value,
                Context context)
                throws IOException, InterruptedException {
            context.write(key, itemList);
        }
    }
}
