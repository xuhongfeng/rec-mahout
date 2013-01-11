/**
 * 2013-1-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.eval;

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
public class CoverageEvaluator implements TopNEvaluator {

    @Override
    public double evaluate(Recommender recommender, DataModel totalDataModel
            ,DataModel testDataModel, int N)
            throws TasteException {
        int numItems = totalDataModel.getNumItems();
        FastIDSet recommendIdSet = new FastIDSet();
        LongPrimitiveIterator it = testDataModel.getUserIDs();
        while (it.hasNext()) {
            long userId = it.nextLong();
            List<RecommendedItem> recommendItems = recommender.recommend(userId, N);
            for (RecommendedItem item:recommendItems) {
                recommendIdSet.add(item.getItemID());
            }
        }
        return recommendIdSet.size()/(numItems*1.0f);
    }
}
