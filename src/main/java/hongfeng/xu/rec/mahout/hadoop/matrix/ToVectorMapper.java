/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorMapper extends Mapper<IntIntWritable, DoubleWritable, IntWritable,
        IntDoubleWritable> {
    
    public static final int TYPE_FIRST = 0;
    public static final int TYPE_SECOND = TYPE_FIRST + 1;
    
    private final IntWritable intWritable = new IntWritable();
    private final IntDoubleWritable intDoubleWritable = new IntDoubleWritable();

    public ToVectorMapper() {
        super();
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int vectorCount = context.getConfiguration().getInt("vectorCount", 0);
        for (int i=0; i<vectorCount; i++) {
            intWritable.set(i);
            context.write(intWritable, IntDoubleWritable.NONE);
        }
    }

    @Override
    protected void map(IntIntWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        int id1 = key.getId1();
        int id2 = key.getId2();
        double v = value.get();
        
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
