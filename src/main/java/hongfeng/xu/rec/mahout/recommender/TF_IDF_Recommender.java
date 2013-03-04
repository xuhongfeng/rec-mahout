/**
 * 2013-3-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.tag.IDF;
import hongfeng.xu.rec.mahout.tag.TF;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author xuhongfeng
 *
 */
public class TF_IDF_Recommender extends BaseRecommender {
    private final TF tf;
    private final IDF idf;

    public TF_IDF_Recommender(DeliciousDataModel dataModel) {
        super(dataModel);
        tf = new TF(getDataModel());
        idf = new IDF(getDataModel());
    }

    @Override
    protected float calculatePreference(long userId, long bookmarkId) throws TasteException {
        double value = 0;
        FastByIDMap<Double> map = new FastByIDMap<Double>();
        PreferenceArray prefs = getDataModel().getUserTagPrefArray(userId);
        double total = 0.0;
        for (Preference pref:prefs) {
            double v = pref.getValue();
            map.put(pref.getItemID(), v);
            total += v;
        }
        LongPrimitiveIterator tagIdsIterator = map.keySetIterator();
        while (tagIdsIterator.hasNext()) {
            long tagId = tagIdsIterator.nextLong();
            value += map.get(tagId)/total * tf.getTf(bookmarkId, tagId) * idf.getIdf(tagId);
        }
        return (float) value;
    }
}
