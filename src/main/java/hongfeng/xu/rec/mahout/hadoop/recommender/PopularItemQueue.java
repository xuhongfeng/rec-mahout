/**
 * 2013-3-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.VarLongWritable;

/**
 * @author xuhongfeng
 *
 */
public class PopularItemQueue {
    private static final int STEP = DeliciousDataConfig.TOP_N;
    private List<Item> list = new ArrayList<Item>();
    private Configuration conf;
    
    private PopularItemQueue(Configuration conf) {
        this.conf = conf;
    }
    
    public long getItemId(int index) throws IOException {
        if (index >= list.size()) {
            loadMore(STEP);
        }
        return list.get(index).itemId;
    }
    
    public static PopularItemQueue create(Configuration conf) throws IOException {
        PopularItemQueue queue = new PopularItemQueue(conf);
        queue.loadMore(2*STEP);
        return queue;
    }
    
    private void loadMore(int size) throws IOException {
        Path path = DeliciousDataConfig.getPopularItemSortPath();
        SequenceFileDirIterator<VarLongWritable, DoubleWritable> iterator
            = new SequenceFileDirIterator<VarLongWritable, DoubleWritable> (
            path, PathType.LIST, PathFilters.partFilter(), null, false, conf);
        try {
            for (int i=0; i<list.size() && iterator.hasNext(); i++) {
                iterator.next();
            }
            for (int i=0; i<size && iterator.hasNext(); i++) {
                Pair<VarLongWritable, DoubleWritable> pair = iterator.next();
                Item item = new Item(pair.getFirst().get(), pair.getSecond().get());
                list.add(item);
            }
        } finally {
            iterator.close();
        }
    }
    
    private static class Item {
        public final long itemId;
        public final double value;
        
        public Item(long itemId, double value) {
            super();
            this.itemId = itemId;
            this.value = value;
        }
    }

}
