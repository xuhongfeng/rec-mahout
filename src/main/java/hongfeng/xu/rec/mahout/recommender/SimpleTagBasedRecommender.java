/**
 * 2013-3-1
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

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

    public SimpleTagBasedRecommender(DeliciousDataModel dataModel) {
        super(dataModel);
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany,
            IDRescorer rescorer) throws TasteException {
        Iterator<Long> bookmarkIdIterator = getDataModel().getBookmarkIds();
        InnerPriorityQueue queue = new InnerPriorityQueue(howMany);
        while (bookmarkIdIterator.hasNext()) {
            long bookmarkID = bookmarkIdIterator.next();
            if (getDataModel().getPreferenceValue(userID, bookmarkID) == null) {
                queue.add(new GenericRecommendedItem(bookmarkID, calculatePreference(userID, bookmarkID)));
            }
        }
        List<RecommendedItem> result = Lists.newArrayList(howMany);
        for (RecommendedItem item:queue) {
            result.add(item);
        }
        Collections.sort(result, DECR_COMPARATOR);
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
            Float bookmarkTagValue = getDataModel().getBookmarkTagValue(bookmarkID, tagId);
            if (bookmarkTagValue != null) {
                value += userTagPref.getValue()*bookmarkTagValue;
            }
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
    
    private static class InnerPriorityQueue extends PriorityQueue<RecommendedItem> {
        private static final long serialVersionUID = -2407213710283867159L;
        
        private final int size;
        public InnerPriorityQueue(int size) {
            super(size, INCR_COMPARATOR);
            this.size = size;
        }
        
        @Override
        public boolean add(RecommendedItem e) {
            if (size()==size) {
                if (INCR_COMPARATOR.compare(peek(), e)<0) {
                    poll();
                    return super.add(e);
                } else {
                    return false;
                }
            } else {
                return super.add(e);
            }
        }
    }
    
    private static Comparator<RecommendedItem> INCR_COMPARATOR = new Comparator<RecommendedItem>() {
        @Override
        public int compare(RecommendedItem o1, RecommendedItem o2) {
            if (o1.getValue() < o2.getValue()) {
                return -1;
            } else {
                return 1;
            }
        }
    };
    private static Comparator<RecommendedItem> DECR_COMPARATOR = new Comparator<RecommendedItem>() {
        @Override
        public int compare(RecommendedItem o1, RecommendedItem o2) {
            return -INCR_COMPARATOR.compare(o1, o2);
        }
    };
}
