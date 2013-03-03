/**
 * 2013-1-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.model.MovielensModel;
import hongfeng.xu.rec.mahout.recommender.PreCachingRecommender;
import hongfeng.xu.rec.mahout.runner.AbsHitRateRunner;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemAverageRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;


/**
 * @author xuhongfeng
 *
 */
public class EvaluateMovielens extends BaseEvaluate {
    
    private Recommender randomRecommender;
    private Recommender itemAverageRecommender;
    private Recommender itemBasedRecommender;
    private Recommender userBasedRecommender;
    private Recommender slopeOneRecommender;
    
    @Override
    protected void initRecommender() {
        PearsonCorrelationSimilarity pearsonSimilarity = null;
        try {
            pearsonSimilarity = new PearsonCorrelationSimilarity(trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        CachingItemSimilarity itemSimilarity = null;
        try {
            itemSimilarity = new CachingItemSimilarity(pearsonSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        CachingUserSimilarity userSimilarity = null;
        try {
            userSimilarity = new CachingUserSimilarity(pearsonSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        
        L.i(this, "build recommender");
        itemBasedRecommender = null;
        try {
            Recommender originRecommender= new GenericItemBasedRecommender(trainingDataModel,
                    itemSimilarity);
//            Recommender originRecommender = new KnnItemBasedRecommender(trainingDataModel,
//                    itemSimilarity, new NonNegativeQuadraticOptimizer(), 20);
            itemBasedRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        NearestNUserNeighborhood neighborhood = null;
        try {
//            neighborhood = new NearestNUserNeighborhood(20,
//                    userSimilarity, trainingDataModel);
            neighborhood = new NearestNUserNeighborhood(trainingDataModel.getNumUsers(),
                    userSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        userBasedRecommender = null;
        try {
            Recommender originRecommender = new GenericUserBasedRecommender(trainingDataModel, neighborhood, userSimilarity);
            userBasedRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        try {
            randomRecommender = createRandomRecommender();
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        itemAverageRecommender = null;
        try {
            ItemAverageRecommender originRecommender = new ItemAverageRecommender(trainingDataModel);
            itemAverageRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        slopeOneRecommender = null;
        try {
//            File diffFile = new File("slopeOne.diff");
//            diffFile.delete();
//            diffFile.createNewFile();
//            SlopeOneRecommender originRecommender = new SlopeOneRecommender(trainingDataModel, Weighting.WEIGHTED,
//                    Weighting.WEIGHTED, new FileDiffStorage(diffFile, Long.MAX_VALUE));
            SlopeOneRecommender originRecommender = new SlopeOneRecommender(trainingDataModel);
            slopeOneRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (Throwable e) {
            L.e(this, e);
            return;
        }
    }
    
    @Override
    public void exec() {
        super.exec();
        L.i(this, "\n\n******************* itemBased recall rate *****************\n\n");
        evaluateRecallRate(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased recall rate *****************\n\n");
        evaluateRecallRate(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender recall rate *****************\n\n");
        evaluateRecallRate(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender recall rate *****************\n\n");
        evaluateRecallRate(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender recall rate *****************\n\n");
        evaluateRecallRate(slopeOneRecommender, "slope one");
        drawChart(recallResult, "recall rate", "recall rate", "recallRate.png", true);
        
        L.i(this, "\n\n******************* itemBased precision rate *****************\n\n");
        evaluatePrecisionRate(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased precision rate *****************\n\n");
        evaluatePrecisionRate(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender precision rate *****************\n\n");
        evaluatePrecisionRate(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender precision rate *****************\n\n");
        evaluatePrecisionRate(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender precision rate *****************\n\n");
        evaluatePrecisionRate(slopeOneRecommender, "slope one");
        drawChart(precisionResult, "precision rate", "precision rate", "precisionRate.png", true);
        
        L.i(this, "\n\n******************* itemBased coverage rate *****************\n\n");
        evaluateCoverageRate(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased coverage rate *****************\n\n");
        evaluateCoverageRate(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender coverage rate *****************\n\n");
        evaluateCoverageRate(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender coverage rate *****************\n\n");
        evaluateCoverageRate(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender coverage rate *****************\n\n");
        evaluateCoverageRate(slopeOneRecommender, "slope one");
        drawChart(coverageResult, "coverage rate", "coverage rate", "coverageRate.png", true);
        
        L.i(this, "\n\n******************* itemBased popularity *****************\n\n");
        evaluatePopularity(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased popularity *****************\n\n");
        evaluatePopularity(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender popularity *****************\n\n");
        evaluatePopularity(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender popularity *****************\n\n");
        evaluatePopularity(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender popularity *****************\n\n");
        evaluatePopularity(slopeOneRecommender, "slope one");
        drawChart(popularityResult, "popularity", "polularity", "popularity.png", false);
    }

    public static void main(String[] args) {
        EvaluateMovielens evaluator = new EvaluateMovielens();
        evaluator.init();
        evaluator.exec();
    }

    @Override
    protected DataModel createDataModel() throws IOException {
        return new MovielensModel();
    }
}
