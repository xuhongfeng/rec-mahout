/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorMapper extends Mapper<LongWritable, Text, IntWritable, IntDoubleWritable> {
    
    public static final int TYPE_FIRST = 0;
    public static final int TYPE_SECOND = TYPE_FIRST + 1;
    
    private final IntWritable intWritable = new IntWritable();
    private final IntDoubleWritable intDoubleWritable = new IntDoubleWritable();

    public ToVectorMapper() {
        super();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] ss = value.toString().split("\\s");
        int id1 = Integer.valueOf(ss[0]);
        int id2 = Integer.valueOf(ss[1]);
        double v = Double.valueOf(ss[2]);
        
        int type = Integer.valueOf(context.getConfiguration().get("type"));
        if (type == TYPE_FIRST) {
            intWritable.set(id1);
            intDoubleWritable.setId(id2);
        } else {
            intWritable.set(id2);
            intDoubleWritable.setId(id1);
        }
        intDoubleWritable.setValue(v);
        
        context.write(intWritable, intDoubleWritable);
    }
}
