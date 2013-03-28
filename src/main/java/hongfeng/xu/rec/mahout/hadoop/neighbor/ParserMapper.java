/**
 * 2013-3-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;
import hongfeng.xu.rec.mahout.hadoop.neighbor.BaseIndexMap.IndexType;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public final class ParserMapper extends Mapper<LongWritable, Text, IntWritable, IntIntWritable> {
    private IdIndexMap itemMap;
    private IdIndexMap userMap;
    
    private IntWritable keyWritable = new IntWritable();
    private IntIntWritable valueWritable = new IntIntWritable();
    
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
        int rate = Integer.valueOf(ss[2]);
        
        int userIndex = userMap.getIndex(userId);
        int itemIndex = itemMap.getIndex(itemId);
        
        keyWritable.set(userIndex);
        valueWritable.setId1(itemIndex);
        valueWritable.setId2(rate);
        
        context.write(keyWritable, valueWritable);
    }
}