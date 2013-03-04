/**
 * 2013-3-1
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author xuhongfeng
 *
 */
public class SimpleTagBasedRecommender extends BaseRecommender {

    public SimpleTagBasedRecommender(DeliciousDataModel dataModel) {
        super(dataModel);
    }

    @Override
    protected float calculatePreference(long userID, long bookmarkID) throws TasteException {
        float value = 0;
        PreferenceArray userTagPrefArray = getDataModel().getUserTagPrefArray(userID);
        for (Preference userTagPref:userTagPrefArray) {
            long tagId = userTagPref.getItemID();
            Float bookmarkTagValue = getDataModel().getBookmarkTagValue(bookmarkID, tagId);
            if (bookmarkTagValue != null) {
                value += userTagPref.getValue()*bookmarkTagValue;
            }
        }
        return value;
    }
}
