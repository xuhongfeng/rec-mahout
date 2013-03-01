/**
 * 2013-3-1
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import com.beust.jcommander.internal.Lists;

/**
 * @author xuhongfeng
 *
 */
public class SimpleTagBasedRecommender extends AbstractRecommender {

    protected SimpleTagBasedRecommender(DeliciousDataModel dataModel) {
        super(dataModel);
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany,
            IDRescorer rescorer) throws TasteException {
        
        Iterator<Long> bookmarkIdIterator = getDataModel().getBookmarkIds();
        Set<RecommendedItem> items = new TreeSet<RecommendedItem>(COMPARATOR);
        while (bookmarkIdIterator.hasNext()) {
            long bookmarkID = bookmarkIdIterator.next();
            if (getDataModel().getPreferenceValue(userID, bookmarkID) == null) {
                items.add(new GenericRecommendedItem(bookmarkID, estimatePreference(userID, bookmarkID)));
            }
        }
        List<RecommendedItem> result = Lists.newArrayList(howMany);
        for (RecommendedItem item:items) {
            result.add(item);
            if (result.size() == howMany) {
                break;
            }
        }
        
        return result;
    }

    @Override
    public float estimatePreference(long userID, long itemID)
            throws TasteException {
        Float value = getDataModel().getPreferenceValue(userID, itemID);
        return value==null?calculatePreference(userID, itemID):value;
    }
    
    private float calculatePreference(long userID, long bookmarkID) throws TasteException {
        float value = 0;
        PreferenceArray userTagPrefArray = getDataModel().getUserTagPrefArray(userID);
        for (Preference userTagPref:userTagPrefArray) {
            long tagId = userTagPref.getItemID();
            float bookmarkTagValue = getDataModel().getBookmarkTagValue(bookmarkID, tagId);
            value += userTagPref.getValue()*bookmarkTagValue;
        }
        return value;
    }
    
    @Override
    public DeliciousDataModel getDataModel() {
        return (DeliciousDataModel) super.getDataModel();
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        getDataModel().refresh(alreadyRefreshed);
    }
    
    private static Comparator<RecommendedItem> COMPARATOR = new Comparator<RecommendedItem>() {
        @Override
        public int compare(RecommendedItem o1, RecommendedItem o2) {
            if (o1.getItemID() == o2.getItemID()) {
                return 0;
            }
            if (o1.getValue() > o2.getValue()) {
                return -1;
            }
            return 1;
        }
    };
}
