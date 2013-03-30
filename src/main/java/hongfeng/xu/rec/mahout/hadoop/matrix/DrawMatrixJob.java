/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class DrawMatrixJob extends AbstractJob {
    public static int MODE_NON_ZERO = 0;
    public static int MODE_WITH_ZERO = MODE_NON_ZERO + 1;
    
    private final int mode;
    private final float precision;
    private final String imageFile;
    private final String title;
    private final String[] subTitles;
    
    public DrawMatrixJob(int mode, float precision, String imageFile,
            String title, String[] subTitles) {
        super();
        this.mode = mode;
        this.precision = precision;
        this.imageFile = imageFile;
        this.title = title;
        this.subTitles = subTitles;
    }

    @Override
    public int run(String[] args) throws Exception {
        
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        AtomicInteger currentPhase = new AtomicInteger();
        
        Path vectorPath = new Path(getInputPath(), "rowVector");
        Path distrubutionPath = new Path(getOutputPath(), "distribution");
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(distrubutionPath, getConf())) {
                Job job = prepareJob(vectorPath, distrubutionPath, MultipleInputFormat.class,
                        MyMapper.class, DoubleWritable.class, IntWritable.class,
                        MyReducer.class, DoubleWritable.class, IntWritable.class,
                        SequenceFileOutputFormat.class);
                job.getConfiguration().setInt("mode", mode);
                job.getConfiguration().setFloat("precision", precision);
                job.setNumReduceTasks(10);
                job.setCombinerClass(MyReducer.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        MatrixDrawer drawer = new MatrixDrawer(distrubutionPath, imageFile, title,
                precision, subTitles);
        drawer.draw(getConf());
        
        return 0;
    }

    public static class MyMapper extends Mapper<IntWritable, VectorWritable, DoubleWritable,
        IntWritable> {
        private DoubleWritable keyWritable = new DoubleWritable();
        private static final IntWritable ONE = new IntWritable(1);

        public MyMapper() {
            super();
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            float precision = context.getConfiguration().getFloat("precision", 0.001f);
            int mode = context.getConfiguration().getInt("mode", MODE_WITH_ZERO);
            
            int i = key.get();
            Vector vector = value.get();
            Iterator<Element> iterator = null;
            if (mode == MODE_NON_ZERO) {
                iterator = vector.iterateNonZero();
            } else {
                iterator = vector.iterator();
            }
            while (iterator.hasNext()) {
                Element e = iterator.next();
                if (e.index() > i) {
                    double v = e.get();
                    int n = (int) (v/precision);
                    keyWritable.set(n*precision);
                    context.write(keyWritable, ONE);
                }
            }
        }
    }
    
    public static class MyReducer extends Reducer<DoubleWritable, IntWritable,
        DoubleWritable, IntWritable> {
        private IntWritable valueWritable = new IntWritable();

        public MyReducer() {
            super();
        }
        
        @Override
        protected void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v:values) {
                count += v.get();
            }
            valueWritable.set(count);
            context.write(key, valueWritable);
        }
    }
}