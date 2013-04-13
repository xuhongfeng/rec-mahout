/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class RandomRecommender extends BaseRecommender {
    
    @Override
    protected int innerRun() throws Exception {
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
        
        private Random random = new Random();
        private int numItems;
        
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            numItems = HadoopUtil.readInt(DataSetConfig.getItemCountPath(),
                    context.getConfiguration());
        }

        public MyMapper() {
            super();
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value,
                Context context)
                throws IOException, InterruptedException {
            int n = DataSetConfig.TOP_N;
            Set<Integer> set = new HashSet<Integer>();
            while (set.size() < n) {
                int itemId = random.nextInt(numItems);
                set.add(itemId);
            }
            List<RecommendedItem> items = new ArrayList<RecommendedItem>();
            for (int id:set) {
                items.add(new RecommendedItem(id, 1.0));
            }
            context.write(key, new RecommendedItemList(items));
        }
    }
}
