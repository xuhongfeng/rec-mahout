/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class RecommendJob extends AbstractJob {
    
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
            if (!HadoopHelper.isFileExists(getOutputPath(), getConf())) {
                Job job = prepareJob(DeliciousDataConfig.getUserItemVectorPath(),
                        getOutputPath(), MultipleInputFormat.class,
                        RecommendMapper.class, IntWritable.class, RecommendedItemList.class,
                        SequenceFileOutputFormat.class);
                job.getConfiguration().set("recommendVectorPath", getInputPath().toString());
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
        private int itemCount;
        
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
        private Path recommendVectorPath;
        
        public RecommendMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            userCount = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(),
                    context.getConfiguration());
            itemCount = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(),
                    context.getConfiguration());
            recommendVectorPath = new Path(context.getConfiguration().get("recommendVectorPath"));
            vectorCache = VectorCache.create(userCount, itemCount, recommendVectorPath,
                    context.getConfiguration());
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            queue.clear();
            
            Vector originVector = value.get();
            int userId = key.get();
            Vector uiVector = vectorCache.get(userId);
            
            Iterator<Element> iterator = uiVector.iterateNonZero();
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
