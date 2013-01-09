/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner.movielens;

import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.runner.AbsPrecisionlRateRunner;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public class PrecisionRateRunner extends AbsPrecisionlRateRunner {

    public PrecisionRateRunner(PrecisionRateEvaluator evaluator,
            Recommender recommender, DataModel testDataModel) {
        super(evaluator, recommender, testDataModel);
    }
}
