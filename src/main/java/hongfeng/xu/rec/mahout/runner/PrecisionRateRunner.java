/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;


/**
 * @author xuhongfeng
 *
 */
public class PrecisionRateRunner extends AbsHitRateRunner<PrecisionRateEvaluator> {
    private static final String RATE_NAME = "precision";

    public PrecisionRateRunner(PrecisionRateEvaluator evaluator,
            Recommender recommender, DataModel totalDataModel,
            DataModel testDataModel) {
        super(evaluator, recommender, totalDataModel, testDataModel, RATE_NAME);
    }
}
