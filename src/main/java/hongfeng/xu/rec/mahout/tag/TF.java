/**
 * 2013-3-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author xuhongfeng
 *
 */
public class TF {
    private final DeliciousDataModel dataModel;
    private final Map<Integer, Double> cache = new HashMap<Integer, Double>();

    public TF(DeliciousDataModel dataModel) {
        super();
        this.dataModel = dataModel;
    }
    
    public double getTf(long bookmarkId, long tagId) throws TasteException {
        int hash = hash(bookmarkId, tagId);
//        L.i(this, "hash:" + hash);
        Double tf = cache.get(hash);
        if (tf == null) {
            tf = calculateTf(bookmarkId, tagId);
            cache.put(hash, tf);
        }
        return tf;
    }
    
    private Map<Long, Double> totalCache = new HashMap<Long, Double>();
    private double calculateTf(long bookmarkId, long tagId) throws TasteException {
        Float value = dataModel.getBookmarkTagValue(bookmarkId, tagId);
        if (value == null) {
            return 0;
        } else {
            PreferenceArray prefs = dataModel.getBookmarkTagPrefArray(bookmarkId);
            Double total = totalCache.get(bookmarkId);
            if (total == null) {
                total = 0.0;
                for (Preference pref:prefs) {
                    total += pref.getValue();
                }
                totalCache.put(bookmarkId, total);
            }
            return value/total;
        }
    }
    
    private static final int PRIME = 31;
    private int hash(long bookmarkId, long tagId) {
        int result = 1;
        result = PRIME*result + (int)(bookmarkId);
        result = PRIME*result + (int)(tagId);
        return result;
    }
}
