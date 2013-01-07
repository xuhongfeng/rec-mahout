/**
 * 2013-1-5
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.eval;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 * @author xuhongfeng
 *
 */
public class ItemBasedRecommenderBuilder implements RecommenderBuilder {

    public Recommender buildRecommender(DataModel dataModel)
            throws TasteException {
        ItemSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
        return new GenericItemBasedRecommender(dataModel, similarity);
    }

}
