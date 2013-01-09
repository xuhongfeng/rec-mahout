/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.recommender.movielens;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;

/**
 * @author xuhongfeng
 *
 */
public class ItemBasedPearsonRecommender extends GenericItemBasedRecommender {

    public ItemBasedPearsonRecommender(DataModel dataModel) throws TasteException {
        super(dataModel, new PearsonCorrelationSimilarity(dataModel));
    }
    
}
