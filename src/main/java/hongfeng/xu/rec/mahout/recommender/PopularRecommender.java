/**
 * 2013-2-23
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import com.google.common.collect.Lists;

/**
 * @author xuhongfeng
 *
 */
public class PopularRecommender extends AbstractRecommender {
    private List<RecommendedItem> popularItems;

    public PopularRecommender(DataModel dataModel,
            CandidateItemsStrategy candidateItemsStrategy) {
        super(dataModel, candidateItemsStrategy);
        init();
    }

    public PopularRecommender(DataModel dataModel) {
        super(dataModel);
        init();
    }
    
    protected void init() {
        try {
            popularItems = Lists.newArrayListWithCapacity(getDataModel().getNumItems());
            LongPrimitiveIterator it = getDataModel().getItemIDs();
            while (it.hasNext()) {
                long itemId = it.next();
                popularItems.add(new GenericRecommendedItem(itemId, getPopularPreference(itemId)));
            }
            Collections.sort(popularItems, COMPARATOR);
        } catch (TasteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany,
            IDRescorer rescorer) throws TasteException {
        List<RecommendedItem> result = Lists.newArrayListWithCapacity(howMany);
        Iterator<RecommendedItem> it = popularItems.iterator();
        while (result.size() != howMany) {
            RecommendedItem item = it.next();
            if (getDataModel().getPreferenceValue(userID, item.getItemID()) == null) {
                result.add(item);
            }
        }
        return result;
    }

    private Map<Long, Float> preferenceCache = new HashMap<Long, Float>();
    private float getPopularPreference(long itemID) throws TasteException {
        if (preferenceCache.containsKey(itemID)) {
            return preferenceCache.get(itemID);
        }
        PreferenceArray preferences = getDataModel().getPreferencesForItem(itemID);
        float result = 0;
        for (Preference p:preferences) {
            result += p.getValue();
        }
        preferenceCache.put(itemID, result);
        return result;
    }
    
    @Override
    public float estimatePreference(long userID, long itemID)
            throws TasteException {
        Float preference = getDataModel().getPreferenceValue(userID, itemID);
        return preference==null?getPopularPreference(itemID):preference;
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        getDataModel().refresh(alreadyRefreshed);
    }

    private static final Comparator<RecommendedItem> COMPARATOR = new Comparator<RecommendedItem>() {
        @Override
        public int compare(RecommendedItem o1, RecommendedItem o2) {
            if (o1.getValue() > o2.getValue()) {
                return -1;
            } else if (o1.getValue() == o2.getValue()) {
                return 0;
            }
            return 1;
        }
        
    };
}
