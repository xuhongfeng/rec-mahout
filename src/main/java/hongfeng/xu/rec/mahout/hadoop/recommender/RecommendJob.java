/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class RecommendJob extends BaseJob {
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.set("recommendVectorPath", getInputPath().toString());
    }
    
    @Override
    protected int innerRun() throws Exception {
        Job job = prepareJob(DataSetConfig.getUserItemVectorPath(),
                getOutputPath(), MultipleInputFormat.class,
                RecommendMapper.class, IntWritable.class, RecommendedItemList.class,
                SequenceFileOutputFormat.class);
        if (!job.waitForCompletion(true)) {
            return -1;
        }
        return 0;
    }

    public static class RecommendMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, RecommendedItemList> {
        private VectorCache vectorCache;
        private int userCount;
        private int itemCount;
        
        private FixedSizePriorityQueue<RecommendedItem> queue = new FixedSizePriorityQueue<RecommendedItem>(
                DataSetConfig.TOP_N, new Comparator<RecommendedItem>() {
                    @Override
                    public int compare(RecommendedItem o1, RecommendedItem o2) {
                        if (o1.getValue() < o2.getValue()) {
                            return -1;
                        }
                        if (o1.getValue() > o2.getValue()) {
                            return 1;
                        }
                        return 0;
                    }
                });
        
        private RecommendedItemList recommendedItemList = new RecommendedItemList();
        private Path recommendVectorPath;
        
        public RecommendMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(),
                    context.getConfiguration());
            itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(),
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
            
            Iterator<Element> iterator = uiVector.iterator();
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
            if (recommendedItemList.size() != DataSetConfig.TOP_N) {
                throw new RuntimeException();
            }
            Collections.sort(recommendedItemList.getItems(), COMPARATOR);
            context.write(key, recommendedItemList);
        }
    }
    
    public static final Comparator<RecommendedItem> COMPARATOR = new Comparator<RecommendedItem>() {
        
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
    };
}
