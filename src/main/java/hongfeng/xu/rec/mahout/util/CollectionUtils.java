/**
 * 2013-2-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author xuhongfeng
 *
 */
public class CollectionUtils {
    public static long[] toArray(Iterator<Long> it) {
        List<Long> list = toList(it);
        return toArray(list);
    }
    
    public static List<Long> toList(Iterator<Long> it) {
        List<Long> list = new ArrayList<Long>();
        while (it.hasNext()) {
            list.add(it.next());
        }
        return list;
    }
    
    public static long[] toArray(Collection<Long> collection) {
        long[] array = new long[collection.size()];
        Iterator<Long> it = collection.iterator();
        int i=0;
        while (it.hasNext()) {
            array[i++] = it.next();
        }
        return array;
    }
}
