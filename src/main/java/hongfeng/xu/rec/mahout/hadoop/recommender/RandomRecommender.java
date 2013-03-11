/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.DataUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class RandomRecommender extends BaseRecommender {
    
    @Override
    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class,
                MyMapper.class, VarLongWritable.class, RecommendedItemsWritable.class,
                SequenceFileOutputFormat.class);
        if (!job.waitForCompletion(true)) {
            return -1;
        }
        return 0;
    }

    public static class MyMapper extends Mapper<VarLongWritable, VectorWritable,
        VarLongWritable, RecommendedItemsWritable> {
        
        private long[] itemIds;
        private Random random = new Random();
        
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            itemIds = DataUtils.parseItemIdSetFromHDFS(context.getConfiguration()).toArray();
        }

        public MyMapper() {
            super();
        }
        
        @Override
        protected void map(VarLongWritable key, VectorWritable value,
                Context context)
                throws IOException, InterruptedException {
            int n = DeliciousDataConfig.TOP_N;
            List<RecommendedItem> items = new ArrayList<RecommendedItem>();
            Vector vector = value.get();
            while (items.size() < n) {
                long itemId = itemIds[random.nextInt(itemIds.length)];
                boolean exists = false;
                for (RecommendedItem item:items) {
                    if (item.getItemID() == itemId) {
                        exists = true;
                        break;
                    }
                }
                if (exists) {
                    continue;
                }
                int index = TasteHadoopUtils.idToIndex(itemId);
                double pref = vector.getQuick(index);
                if (pref != 0) {
                    continue;
                }
                items.add(new GenericRecommendedItem(itemId, 0));
            }
            context.write(key, new RecommendedItemsWritable(items));
        }
    }
}
