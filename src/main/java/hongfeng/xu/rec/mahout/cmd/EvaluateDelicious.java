/**
 * 2013-2-23
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.recommender.SimpleTagBasedRecommender;
import hongfeng.xu.rec.mahout.util.DataModelUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.Pair;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateDelicious extends BaseEvaluate {
    private Recommender popularRecommender;
    private Recommender randomRecommender;
    private Recommender simpleTagBasedRecommender;

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
            SimpleTagBasedRecommender tagBasedRecommender = new SimpleTagBasedRecommender((DeliciousDataModel) trainingDataModel);
            this.simpleTagBasedRecommender = wrapPreCacheRecommender(tagBasedRecommender);
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
        L.i(this, "\n\n******************* simple tag based recommender recall rate *****************\n\n");
        evaluateRecallRate(simpleTagBasedRecommender, "SimpleTagBased");
        drawChart(recallResult, "recall rate", "recall rate", "recallRate.png", true);
        
        L.i(this, "\n\n******************* random recommender precision rate *****************\n\n");
        evaluatePrecisionRate(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender precision rate *****************\n\n");
        evaluatePrecisionRate(popularRecommender, "popular");
        L.i(this, "\n\n******************* simple tag based recommender precision rate *****************\n\n");
        evaluatePrecisionRate(simpleTagBasedRecommender, "SimpleTagBased");
        drawChart(precisionResult, "precision rate", "precision rate", "precisionRate.png", true);
        
        L.i(this, "\n\n******************* random recommender coverage rate *****************\n\n");
        evaluateCoverageRate(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender coverage rate *****************\n\n");
        evaluateCoverageRate(popularRecommender, "popular");
        L.i(this, "\n\n******************* simple tag based recommender coverage rate *****************\n\n");
        evaluateCoverageRate(simpleTagBasedRecommender, "SimpleTagBased");
        drawChart(coverageResult, "coverage rate", "coverage rate", "coverageRate.png", true);
        
        L.i(this, "\n\n******************* random recommender popularity *****************\n\n");
        evaluatePopularity(randomRecommender, "random");
        L.i(this, "\n\n******************* popular recommender popularity *****************\n\n");
        evaluatePopularity(popularRecommender, "popular");
        L.i(this, "\n\n******************* simple tag based recommender popularity *****************\n\n");
        evaluatePopularity(simpleTagBasedRecommender, "SimpleTagBased");
        drawChart(popularityResult, "popularity", "polularity", "popularity.png", false);
    }
    
    @Override
    protected DataModel createDataModel() throws IOException {
        return new DeliciousDataModel(DeliciousDataConfig.RAW_DATA_FILE);
    }
    
    @Override
    protected void initDataModel() {
        try {
            totalDataModel = createDataModel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            Pair<DeliciousDataModel, DeliciousDataModel> pair =
                    DataModelUtils.splitDeliciousDataModel((DeliciousDataModel) totalDataModel,
                    1, 0.8);
            trainingDataModel = pair.getFirst();
            testDataModel = pair.getSecond();
        } catch (TasteException e) {
            throw new RuntimeException(e);
        }
    }
    
    private DeliciousDataModel getTrainingDataMode() {
        return (DeliciousDataModel) trainingDataModel;
    }
}
