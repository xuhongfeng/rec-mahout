/**
 * 2013-3-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.tag.TagAllocator.AllocateItem;
import hongfeng.xu.rec.mahout.util.L;

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

/**
 * @author xuhongfeng
 *
 */
public class TagFlatter {
    public DeliciousDataModel flat(DeliciousDataModel originDataModel) throws TasteException {
        long start = System.currentTimeMillis();
        DataModel bookmarkTagModel = originDataModel.getBookmarkTagModel();
        TagAllocator tagAllocator = new TagAllocator(bookmarkTagModel);
        
        DataModel userTagModel = originDataModel.getUserTagModel();
        FastByIDMap<PreferenceArray> userTagMap = new FastByIDMap<PreferenceArray>();
        LongPrimitiveIterator userIdsIterator = userTagModel.getUserIDs();
        L.i(this, "user id count = " + userTagModel.getNumUsers());
        while (userIdsIterator.hasNext()) {
            long userId = userIdsIterator.nextLong();
            PreferenceArray originPrefArray = userTagModel.getPreferencesFromUser(userId);
            if (originPrefArray.length() == 0) {
                L.i(this, "originPrefArray.length=0");
                continue;
            }
            PreferenceArray newPrefArray = flatPreferenceArray(userId, originPrefArray, tagAllocator);
            userTagMap.put(userId, newPrefArray);
//            L.i(this, "user tag map size = " + userTagMap.size());
        }
        
        DataModel newUserTagModel = new GenericDataModel(userTagMap); 
        DeliciousDataModel newDataModel = new DeliciousDataModel(originDataModel.getRawDataSet());
        newDataModel.setUserTagModel(newUserTagModel);
        
        L.i(this, "flat tag cost = " + (System.currentTimeMillis() - start));
        return newDataModel;
    }
    
    private PreferenceArray flatPreferenceArray(long userId, PreferenceArray prefs, TagAllocator tagAllocator) throws TasteException {
        FastByIDMap<Preference> prefMap = new FastByIDMap<Preference>();
        for (Preference pref:prefs) {
            long tagId = pref.getItemID();
            float value = pref.getValue();
            incr(prefMap, userId, tagId, value);
            List<AllocateItem> items = tagAllocator.allocateTag(tagId);
            for (AllocateItem item:items) {
                incr(prefMap, userId, item.getId(), (float) (value*item.getValue()));
            }
        }
        
        List<Preference> list = new ArrayList<Preference>();
        for (Map.Entry<Long, Preference> entry:prefMap.entrySet()) {
            list.add(entry.getValue());
        }
        PreferenceArray newPrefs = new GenericUserPreferenceArray(list);
        return newPrefs;
    }
    
    private void incr(FastByIDMap<Preference> map, long userId, long tagId, float value) {
        if (value == 0) {
            L.i(this, "value = " + value);
            return;
        }
        Preference pref = map.get(tagId);
        if (pref == null) {
            pref = new GenericPreference(userId, tagId, value);
            map.put(tagId, pref);
        } else {
            pref.setValue(pref.getValue() + value);
        }
    }
}
