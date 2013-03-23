/**
 * 2013-3-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyVectorJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyVectorReducer;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class SimpleTagBasedRecommender extends BaseRecommender {

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
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getUTTIPath(), getConf())) {
                MultiplyVectorJob job = new MultiplyVectorJob(MultiplyVectorReducer.UTTI.class);
                ToolRunner.run(job, new String[] {
                        "--input", DeliciousDataConfig.getUserTagVectorPath().toString(),
                        "--output", DeliciousDataConfig.getUTTIPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getSimpleTagBasedResult(),
                    getConf())) {
                Job job = prepareJob(DeliciousDataConfig.getUserItemVectorPath(),
                        DeliciousDataConfig.getSimpleTagBasedResult(), MultipleInputFormat.class,
                        RecommendMapper.class, IntWritable.class, RecommendedItemList.class,
                        SequenceFileOutputFormat.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }
    
    public static class RecommendMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, RecommendedItemList> {
        private VectorCache vectorCache;
        private int userCount;
        private int tagCount;
        private FixedSizePriorityQueue<RecommendedItem> queue = new FixedSizePriorityQueue<RecommendedItem>(
                DeliciousDataConfig.TOP_N, new Comparator<RecommendedItem>() {
                    @Override
                    public int compare(RecommendedItem o1, RecommendedItem o2) {
                        if (o1.getValue() > o2.getValue()) {
                            return -1;
                        }
                        if (o1.getValue() < o2.getValue()) {
                            return 1;
                        }
                        return 0;
                    }
                });
        private RecommendedItemList recommendedItemList = new RecommendedItemList();
        private Random random = new Random();
        
        public RecommendMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            userCount = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(),
                    context.getConfiguration());
            tagCount = HadoopUtil.readInt(DeliciousDataConfig.getTagCountPath(),
                    context.getConfiguration());
            vectorCache = VectorCache.create(userCount, tagCount, DeliciousDataConfig.getUTTIRowVectorPath(),
                    context.getConfiguration());
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            queue.clear();
            
            Vector originVector = value.get();
            int userId = key.get();
            Vector utti = vectorCache.get(userId);
            
            Iterator<Element> iterator = utti.iterateNonZero();
            while (iterator.hasNext()) {
                Element e = iterator.next();
                int itemId = e.index();
                if (originVector.getQuick(itemId) == 0.0) {
                    double pref = e.get();
                    RecommendedItem item = new RecommendedItem(itemId, pref);
                    queue.add(item);
                }
            }
            
            recommendedItemList.clear();
            recommendedItemList.addAll(queue.toArray(new RecommendedItem[0]));
            while (recommendedItemList.size() != DeliciousDataConfig.TOP_N) {
                int itemId = random.nextInt(originVector.size());
                if (originVector.getQuick(itemId) == 0.0) {
                    RecommendedItem item = new RecommendedItem(itemId, 0.0);
                    recommendedItemList.add(item);
                }
            }
            context.write(key, recommendedItemList);
        }
    }

}
