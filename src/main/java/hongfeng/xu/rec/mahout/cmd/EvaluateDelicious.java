/**
 * 2013-2-23
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.model.DeliciousModel;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateDelicious extends BaseEvaluate {
    private Recommender popularRecommender;
    private Recommender randomRecommender;

    public static void main(String[] args) {
        EvaluateDelicious evaluator = new EvaluateDelicious();
        evaluator.init();
        evaluator.exec();
    }
    
    @Override
    protected void initRecommender() {
        super.initRecommender();
        try {
            popularRecommender = createPopularRecommender();
            randomRecommender = createRandomRecommender();
        } catch (TasteException e) {
            L.e(this, e);
        }
    }
    
    @Override
    public void exec() {
        super.exec();
        L.i(this, "\n\n******************* random recommender recall rate *****************\n\n");
        evaluateRecallRate(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender recall rate *****************\n\n");
        evaluateRecallRate(popularRecommender, "popular");
        drawChart(recallResult, "recall rate", "recall rate", "recallRate.png", true);
        
        L.i(this, "\n\n******************* random recommender precision rate *****************\n\n");
        evaluatePrecisionRate(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender precision rate *****************\n\n");
        evaluatePrecisionRate(popularRecommender, "popular");
        drawChart(precisionResult, "precision rate", "precision rate", "precisionRate.png", true);
        
        L.i(this, "\n\n******************* random recommender coverage rate *****************\n\n");
        evaluateCoverageRate(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender coverage rate *****************\n\n");
        evaluateCoverageRate(popularRecommender, "popular");
        drawChart(coverageResult, "coverage rate", "coverage rate", "coverageRate.png", true);
        
        L.i(this, "\n\n******************* random recommender popularity *****************\n\n");
        evaluatePopularity(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender popularity *****************\n\n");
        evaluatePopularity(popularRecommender, "popular");
        drawChart(popularityResult, "popularity", "polularity", "popularity.png", false);
    }
    
    @Override
    protected DataModel getDataModel() throws IOException {
        return new DeliciousModel();
    }
}
