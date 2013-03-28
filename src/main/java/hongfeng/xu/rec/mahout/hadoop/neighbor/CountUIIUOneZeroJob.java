/**
 * 2013-3-28
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class CountUIIUOneZeroJob extends AbstractJob {

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
            Path output = MovielensDataConfig.getCountUIIUOneZeroPath();
            if (!HadoopHelper.isFileExists(output, getConf())) {
                Path input = new Path(MovielensDataConfig.getUIIUOneZero(), "rowVector");
                Job job = prepareJob(input, output, MultipleInputFormat.class,
                        MyMapper.class, IntWritable.class, IntWritable.class,
                        MyReducer.class, IntWritable.class, IntWritable.class,
                        SequenceFileOutputFormat.class);
                job.setNumReduceTasks(10);
                job.setCombinerClass(MyReducer.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }
    
    public static class MyMapper extends Mapper<IntWritable, VectorWritable, IntWritable, IntWritable> {
        private IntWritable keyWritable = new IntWritable();
        private static final IntWritable ONE = new IntWritable(1);

        public MyMapper() {
            super();
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            int i = key.get();
            Vector vector = value.get();
            Iterator<Element> iterator = vector.iterateNonZero();
            while (iterator.hasNext()) {
                Element e = iterator.next();
                if (e.index() > i) {
                    int count = (int) e.get();
                    keyWritable.set(count);
                    context.write(keyWritable, ONE);
                }
            }
        }
    }
    
    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public MyReducer() {
            super();
        }
        
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable v:values) {
                total += v.get();
            }
            IntWritable valueWritable = new IntWritable(total);
            context.write(key, valueWritable);
        }
    }
}
