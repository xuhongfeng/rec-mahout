/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorMapper extends Mapper<LongWritable, Text, KeyType, IntDoubleWritable> {
    private final KeyType keyType = new KeyType();
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
        
        keyType.setType(KeyType.TYPE_ROW);
        keyType.setIndex(id1);
        intDoubleWritable.setId(id2);
        intDoubleWritable.setValue(v);
        context.write(keyType, intDoubleWritable);
        
        keyType.setType(KeyType.TYPE_COLUMN);
        keyType.setIndex(id2);
        intDoubleWritable.setId(id1);
        intDoubleWritable.setValue(v);
        context.write(keyType, intDoubleWritable);
    }
}
