/**
 * 2013-3-18
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
public class CombineMatrixMapper extends Mapper<IntIntWritable, DoubleWritable,
    IntWritable, IntDoubleWritable> {
    public static final int TYPE_ROW = 1;
    public static final int TYPE_COLUMN = TYPE_ROW + 1;
    
    private IntWritable intWritable = new IntWritable();
    private IntDoubleWritable intDoubleWritable = new IntDoubleWritable();

    public CombineMatrixMapper() {
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
        int row = key.getId1();
        int column = key.getId2();
        
        if (context.getConfiguration().getInt("type", TYPE_ROW) == TYPE_COLUMN) {
            int t = row;
            row = column;
            column = t;
        }
        
        intWritable.set(row);
        intDoubleWritable.setId(column);
        intDoubleWritable.setValue(value.get());
        
        context.write(intWritable, intDoubleWritable);
    }
}
