/**
 * 2013-3-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 * @author xuhongfeng
 *
 */
public class TagFlatter {
    public DeliciousDataModel flat(DeliciousDataModel originDataModel,
            ItemSimilarity tagSimilarity) throws TasteException {
        DataModel userTag = originDataModel.getUserTagModel();
        
        // <userId, newPrefArray>
        FastByIDMap<PreferenceArray> map = new FastByIDMap<PreferenceArray>();
        LongPrimitiveIterator userIdIterator = userTag.getUserIDs();
        while (userIdIterator.hasNext()) {
            long userId = userIdIterator.nextLong();
            PreferenceArray oldPrefArray = originDataModel.getUserTagPrefArray(userId);
            // <tagId, pref>
            FastByIDMap<Preference> prefMap = new FastByIDMap<Preference>();
            for (Preference oldPref:oldPrefArray) {
                long tagId = oldPref.getItemID();
                incr(prefMap, userId, tagId, oldPref.getValue());
                long[] similarTagIds = tagSimilarity.allSimilarItemIDs(tagId);
                double[] similarityValue = tagSimilarity.itemSimilarities(tagId, similarTagIds);
                for (int i=0; i<similarTagIds.length; i++) {
                    long otherTagId = similarTagIds[i];
                    double sim = similarityValue[i];
                    if (sim > 0) {
                        float incr = (float) (oldPref.getValue()*sim);
                        incr(prefMap, userId, otherTagId, incr);
                    }
                }
            }
            List<Preference> newPrefs = new ArrayList<Preference>();
            for (Map.Entry<Long, Preference> entry:prefMap.entrySet()) {
                newPrefs.add(entry.getValue());
            }
            PreferenceArray newArray = new GenericUserPreferenceArray(newPrefs);
            map.put(userId, newArray);
        }
        DataModel newUserTag = new GenericDataModel(map);
        DeliciousDataModel newDataModel = new DeliciousDataModel(originDataModel.getRawDataSet());
        newDataModel.setUserTagModel(newUserTag);
        return newDataModel;
    }
    
    private void incr(FastByIDMap<Preference> map, long userId, long tagId, float value) {
        Preference pref = map.get(tagId);
        if (pref == null) {
            pref = new GenericPreference(userId, tagId, value);
            map.put(tagId, pref);
        }
        pref.setValue(pref.getValue() + value);
    }
}
