/**
 * 2013-1-12
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.PopularityEvaluator;
import hongfeng.xu.rec.mahout.util.L;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public class PopularityRunner extends AbsTopNRunner<PopularityEvaluator> {
    private static final String RATE_NAME = "polularity";

    public PopularityRunner(PopularityEvaluator evaluator,
            Recommender recommender, DataModel totalDataModel,
            DataModel testDataModel) {
        super(evaluator, recommender, totalDataModel, testDataModel, RATE_NAME);
    }
    
    @Override
    protected void reportRate(double rate) {
        L.i(this, RATE_NAME + " = %.2f", rate);
    }
}
