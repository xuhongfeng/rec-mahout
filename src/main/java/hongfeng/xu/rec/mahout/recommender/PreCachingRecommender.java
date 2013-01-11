/**
 * 2013-1-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender;

import java.util.Collection;
import java.util.List;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import com.google.common.base.Preconditions;

/**
 * @author xuhongfeng
 *
 */
public class PreCachingRecommender implements Recommender {
    private final Recommender cachingRecommender;
    private final int PRE_RECOMMEND_SIZE;

    public PreCachingRecommender(Recommender recommender,
            int preRecommendSize) throws TasteException {
        super();
        Preconditions.checkNotNull(recommender);
        Preconditions.checkArgument(preRecommendSize >= 0);
        
        if (recommender instanceof CachingRecommender
                || recommender instanceof PreCachingRecommender) {
            this.cachingRecommender = recommender;
        } else {
            this.cachingRecommender = new CachingRecommender(recommender);
        }
        
        this.PRE_RECOMMEND_SIZE = preRecommendSize;
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany)
            throws TasteException {
        return recommend(userID, howMany, null);
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany,
            IDRescorer rescorer) throws TasteException {
        int recommendSize = howMany < PRE_RECOMMEND_SIZE ? PRE_RECOMMEND_SIZE : howMany;
        List<RecommendedItem> recommendItems = cachingRecommender.recommend(userID,
                recommendSize, rescorer);
        if (recommendItems.size() > howMany) {
            return recommendItems.subList(0, howMany);
        } else {
            return recommendItems;
        }
    }

    @Override
    public float estimatePreference(long userID, long itemID)
            throws TasteException {
        return cachingRecommender.estimatePreference(userID, itemID);
    }

    @Override
    public void setPreference(long userID, long itemID, float value)
            throws TasteException {
        cachingRecommender.setPreference(userID, itemID, value);
    }

    @Override
    public void removePreference(long userID, long itemID)
            throws TasteException {
        cachingRecommender.removePreference(userID, itemID);
    }

    @Override
    public DataModel getDataModel() {
        return cachingRecommender.getDataModel();
    }

}
