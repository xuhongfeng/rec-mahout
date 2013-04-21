/**
 * 2013-4-20
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.analyzer;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;
import hongfeng.xu.rec.mahout.util.CollectionUtils;

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public abstract class BasePredictableRateJob extends BaseJob {
    protected final int[] KList;
    
    public BasePredictableRateJob(int[] kList) {
        super();
        KList = kList;
    }

    @Override
    protected int innerRun() throws Exception {
        Path inputPath = getInputPath();
        Job job = prepareJob(inputPath, getOutputPath(),
                MultipleInputFormat.class, getMapperClass(), IntWritable.class,
                DoubleWritable.class, MyReducer.class, IntWritable.class,
                DoubleWritable.class, SequenceFileOutputFormat.class);
        job.setNumReduceTasks(DataSetConfig.REDUCE_COUNT);
        if (!job.waitForCompletion(true)) {
            return -1;
        }
        return 0;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.set("KList", CollectionUtils.join(KList, ","));
    }
    
    protected abstract Class<? extends PredictableRateMapper> getMapperClass();

    public static class PredictableRateMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, DoubleWritable> {
        protected int[] KList;
        protected FixedSizePriorityQueue<Pair<Double,Integer>> queue;
        
        protected IntWritable keyWritable = new IntWritable();
        protected DoubleWritable valueWritable = new DoubleWritable();

        public PredictableRateMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String s = context.getConfiguration().get("KList");
            String[] ss = s.split(",");
            KList = new int[ss.length];
            HadoopHelper.log(this, s);
            HadoopHelper.log(this, "KList.length = " + KList.length);
            for (int i=0; i<KList.length; i++) {
                KList[i] = Integer.valueOf(ss[i].trim());
            }
            queue = new FixedSizePriorityQueue<Pair<Double, Integer>>(KList[KList.length-1],
                new Comparator<Pair<Double, Integer>>() {
                    @Override
                    public int compare(Pair<Double, Integer> o1,
                            Pair<Double, Integer> o2) {
                        if (o1.getFirst() > o2.getFirst()) {
                            return 1;
                        }
                        if (o1.getFirst() < o2.getFirst()) {
                            return -1;
                        }
                        return 0;
                    }
            });
        }
    }
    
    public static class MyReducer extends Reducer<IntWritable, DoubleWritable,
        IntWritable, DoubleWritable> {

        public MyReducer() {
            super();
        }
        
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            double total = 0.0;
            for (DoubleWritable value:values) {
                count++;
                total += value.get();
            }
            double rate = count==0?0.0:total/count;
            DoubleWritable valueWritable = new DoubleWritable(rate);
            context.write(key, valueWritable);
        }
    }
}
