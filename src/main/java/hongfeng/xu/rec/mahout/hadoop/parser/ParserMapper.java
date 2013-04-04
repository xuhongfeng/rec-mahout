/**
 * 2013-3-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.misc.BaseIndexMap.IndexType;
import hongfeng.xu.rec.mahout.hadoop.misc.IdIndexMap;
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
public final class ParserMapper extends Mapper<LongWritable, Text, IntWritable, IntDoubleWritable> {
    private IdIndexMap itemMap;
    private IdIndexMap userMap;
    
    private IntWritable keyWritable = new IntWritable();
    private IntDoubleWritable valueWritable = new IntDoubleWritable();
    
    public ParserMapper() {
        super();
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        itemMap = IdIndexMap.create(IndexType.ItemIndex, context.getConfiguration());
        userMap = IdIndexMap.create(IndexType.UserIndex, context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] ss = value.toString().split("\\s");
        long userId = Long.valueOf(ss[0]);
        long itemId = Long.valueOf(ss[1]);
        double rate = Double.valueOf(ss[2]);
        
        int userIndex = userMap.getIndex(userId);
        int itemIndex = itemMap.getIndex(itemId);
        
        keyWritable.set(userIndex);
        valueWritable.setId(itemIndex);
        if (DataSetConfig.ONE_ZERO) {
            if (rate >= 3) {
                rate = 1.0;
            } else {
                return;
            }
        } 
        valueWritable.setValue(rate);
        
        context.write(keyWritable, valueWritable);
    }
}