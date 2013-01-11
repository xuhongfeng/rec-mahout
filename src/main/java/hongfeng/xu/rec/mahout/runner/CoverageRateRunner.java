/**
 * 2013-1-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.CoverageEvaluator;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;


/**
 * @author xuhongfeng
 *
 */
public class CoverageRateRunner extends AbsTopNRunner<CoverageEvaluator> {
    private static final String RATE_NAME = "coverage";

    public CoverageRateRunner(CoverageEvaluator evaluator,
            Recommender recommender, DataModel totalDataModel,
            DataModel testDataModel) {
        super(evaluator, recommender, totalDataModel, testDataModel, RATE_NAME);
    }
}
