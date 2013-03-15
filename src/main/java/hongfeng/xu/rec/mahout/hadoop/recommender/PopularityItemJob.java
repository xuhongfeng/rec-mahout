/**
 * 2013-3-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.LongDoubleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntLongHashMap;

/**
 * @author xuhongfeng
 *
 */
public class PopularityItemJob extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
       /**
        * input : itemUserVector, SequenceFileInputFormat 
        * out : itemValuPath, SequenceFileOutputFormat
        * 
        * IntWritable,VectorWritable > Long,Double > Long,Double
        */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(), getConf())) {
                Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class,
                        ParseMapper.class, IntWritable.class, LongDoubleWritable.class,
                        SortItemReducer.class, LongWritable.class, DoubleWritable.class,
                        SequenceFileOutputFormat.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }
    
    private static final Comparator<Item> COMPARATOR = new Comparator<Item>() {
        
        @Override
        public int compare(Item o1, Item o2) {
            if (o1.value > o2.value) {
                return -1;
            }
            if (o1.value < o2.value) {
                return 1;
            }
            return 0;
        }
    };
    
    private static class Item {
        public final long itemId;
        public final double value;
        
        public Item(long itemId, double value) {
            super();
            this.itemId = itemId;
            this.value = value;
        }
    }

    public static class ParseMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, LongDoubleWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        
        private LongDoubleWritable writable = new LongDoubleWritable();
        
        private OpenIntLongHashMap map;

        public ParseMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            map = TasteHadoopUtils.readItemIDIndexMap(DeliciousDataConfig.getUserItemMatrixIndexPath().toString(), context.getConfiguration());
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value,
                Context context)
                throws IOException, InterruptedException {
            Vector vector = value.get();
            writable.setId(map.get(key.get()));
            writable.setValue(vector.zSum());
            context.write(ONE, writable);
        }
    }
    
    public static class SortItemReducer extends Reducer<IntWritable,
        LongDoubleWritable, LongWritable, DoubleWritable> {
        private DoubleWritable doubleWritable = new DoubleWritable();
        private LongWritable longWritable = new LongWritable();

        public SortItemReducer() {
            super();
        }
        
        @Override
        protected void reduce(IntWritable key,
                Iterable<LongDoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Item> list = new ArrayList<Item>();
            for (LongDoubleWritable value:values) {
                list.add(new Item(value.getId(), value.getValue()));
            }
            Collections.sort(list, COMPARATOR);
            for (Item item:list) {
                longWritable.set(item.itemId);
                doubleWritable.set(item.value);
                context.write(longWritable, doubleWritable);
            }
        }
    }
}
