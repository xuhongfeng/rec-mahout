/**
 * 2013-1-12
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.eval;

import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.Cache;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.common.Retriever;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public class PopularityEvaluator implements TopNEvaluator {

    @Override
    public double evaluate(Recommender recommender, DataModel totalDataModel,
            DataModel testDataModel, int N) throws TasteException {
        final DataModel trainingModel = recommender.getDataModel();
        Cache<Long, Integer> popularityCache = new Cache<Long, Integer>(
            new Retriever<Long, Integer>() {
                @Override
                public Integer get(Long itemId) throws TasteException {
                    return trainingModel.getNumUsersWithPreferenceFor(itemId);
                }
        });
        double n = 0;
        double p = 0;
        LongPrimitiveIterator it = testDataModel.getUserIDs();
        while (it.hasNext()) {
            long userId = it.nextLong();
            List<RecommendedItem> recommendItems = recommender.recommend(userId, N);
            n += recommendItems.size();
            for (RecommendedItem item:recommendItems) {
                long itemId = item.getItemID();
                int popularity = popularityCache.get(itemId);
                p += Math.log(1 + popularity);
            }
        }
        return p/n;
    }
}
