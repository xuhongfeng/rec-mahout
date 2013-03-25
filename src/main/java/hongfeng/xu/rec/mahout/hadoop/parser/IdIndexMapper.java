/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public class IdIndexMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    public static final int TYPE_USER_ID = 0;
    public static final int TYPE_ITEM_ID = TYPE_USER_ID + 1;
    public static final int TYPE_TAG_ID = TYPE_ITEM_ID + 1;
    
    private final LongWritable longWritable = new LongWritable();
    private final IntWritable intWritable = new IntWritable();

    public IdIndexMapper() {
        super();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] ss = value.toString().split("\\s");
        long userId = Long.valueOf(ss[0]);
        long itemId = Long.valueOf(ss[1]);
        long tagId = Long.valueOf(ss[2]);
        write(context, TYPE_USER_ID, userId);
        write(context, TYPE_ITEM_ID, itemId);
        write(context, TYPE_TAG_ID, tagId);
    }
    
    private void write(Context context, int key, long value) throws IOException, InterruptedException {
        intWritable.set(key);
        longWritable.set(value);
        context.write(intWritable, longWritable);
    }
}
