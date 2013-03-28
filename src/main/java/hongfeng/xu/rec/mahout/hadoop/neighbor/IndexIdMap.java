/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.map.OpenIntLongHashMap;

/**
 * @author xuhongfeng
 *
 */
public class IndexIdMap extends BaseIndexMap {
    private final OpenIntLongHashMap map = new OpenIntLongHashMap();
    
    private IndexIdMap(IndexType indexType) {
        super(indexType);
    }

    protected void add(int index, long id) {
        map.put(index, id);
    }
    
    public long get(int index) {
        return map.get(index);
    }
    
    public static IndexIdMap create(IndexType type, Configuration conf) throws IOException {
        IndexIdMap map = new IndexIdMap(type);
        map.init(conf);
        return map;
    }
}
