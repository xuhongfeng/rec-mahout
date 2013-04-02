/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
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
        AtomicInteger currentPhase = new AtomicInteger();
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class,
                    MyMapper.class, IntWritable.class, RecommendedItemList.class,
                    SequenceFileOutputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
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
            List<RecommendedItem> items = new ArrayList<RecommendedItem>();
            Vector vector = value.get();
            while (items.size() < n) {
                int itemId = random.nextInt(numItems);
                boolean exists = false;
                for (RecommendedItem item:items) {
                    if (item.getId() == itemId) {
                        exists = true;
                        break;
                    }
                }
                if (exists) {
                    continue;
                }
                double pref = vector.getQuick(itemId);
                if (pref != 0) {
                    continue;
                }
                items.add(new RecommendedItem(itemId, 1.0));
            }
            context.write(key, new RecommendedItemList(items));
        }
    }
}
