/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.map.OpenLongIntHashMap;

/**
 * @author xuhongfeng
 *
 */
public class IdIndexMap extends BaseIndexMap {
    private final OpenLongIntHashMap map = new OpenLongIntHashMap();

    protected IdIndexMap(IndexType indexType) {
        super(indexType);
    }
    
    public int getIndex(long id) {
        return map.get(id);
    }

    @Override
    protected void add(int index, long id) {
        map.put(id, index);
    }
    
    public static IdIndexMap create(IndexType type, Configuration conf) throws IOException {
        IdIndexMap map = new IdIndexMap(type);
        map.init(conf);
        return map;
    }
}
