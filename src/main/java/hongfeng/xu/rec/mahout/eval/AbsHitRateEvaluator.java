/**
 * 2013-1-6
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.eval;

import hongfeng.xu.rec.mahout.util.L;

import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public abstract class AbsHitRateEvaluator implements TopNEvaluator {

    @Override
    public double evaluate(Recommender recommender, DataModel testModel, int N)
            throws TasteException {
        int all = 0;
        int hit = 0;
        
        LongPrimitiveIterator it = testModel.getUserIDs();
        while(it.hasNext()) {
            long userId = it.next();
            FastIDSet userItemIds = testModel.getItemIDsFromUser(userId);
            L.i(this, "recommend in");
            List<RecommendedItem> recommendItems = recommender.recommend(userId, N);
            L.i(this, "recommend out");
            for (RecommendedItem item:recommendItems) {
                if (userItemIds.contains(item.getItemID())) {
                    hit++;
                }
            }
            all = calculateAll(all, recommendItems.size(), userItemIds.size());
        }
        return hit/(all*1.0);
    }
    
    protected abstract int calculateAll(int oldAll, int recomendSize, int userItemSize);
}
