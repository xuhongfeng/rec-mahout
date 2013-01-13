/**
 * 2013-1-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.eval.CoverageEvaluator;
import hongfeng.xu.rec.mahout.eval.PopularityEvaluator;
import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.model.MovielensModel;
import hongfeng.xu.rec.mahout.recommender.PreCachingRecommender;
import hongfeng.xu.rec.mahout.runner.AbsHitRateRunner;
import hongfeng.xu.rec.mahout.runner.CoverageRateRunner;
import hongfeng.xu.rec.mahout.runner.PopularityRunner;
import hongfeng.xu.rec.mahout.runner.PrecisionRateRunner;
import hongfeng.xu.rec.mahout.runner.RecallRateRunner;
import hongfeng.xu.rec.mahout.util.DataModelUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.Pair;


/**
 * @author xuhongfeng
 *
 */
public class Evaluator {

    public static void main(String[] args) {
        
        /* step 1. create data model */
        L.i("Main", "create data model");
        File file = new File("data/u.data");
        MovielensModel totalDataModel = null;
        try {
            totalDataModel = new MovielensModel(file);
        } catch (IOException e) {
            L.e("main", e);
            return;
        }
        
        /* step 2. split data model */
        L.i("Main", "split data model");
        DataModel trainingDataModel = null;
        DataModel testDataModel = null;
        try {
            Pair<DataModel, DataModel> models =
                    DataModelUtils.split(totalDataModel, 0.1, 0.8);
            trainingDataModel = models.getFirst();
            testDataModel = models.getSecond();
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        /* step 3.  build similarity */
        PearsonCorrelationSimilarity pearsonSimilarity = null;
        try {
            pearsonSimilarity = new PearsonCorrelationSimilarity(trainingDataModel);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        CachingItemSimilarity itemSimilarity = null;
        try {
            itemSimilarity = new CachingItemSimilarity(pearsonSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        CachingUserSimilarity userSimilarity = null;
        try {
            userSimilarity = new CachingUserSimilarity(pearsonSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        /* step 4. build Recommender */
        L.i("Main", "build recommender");
        PreCachingRecommender itemBasedRecommender = null;
        try {
            Recommender originRecommender= new GenericItemBasedRecommender(trainingDataModel,
                    itemSimilarity);
//            Recommender originRecommender = new KnnItemBasedRecommender(trainingDataModel,
//                    similarity, new NonNegativeQuadraticOptimizer(), 20);
            itemBasedRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        NearestNUserNeighborhood neighborhood = null;
        try {
            neighborhood = new NearestNUserNeighborhood(trainingDataModel.getNumUsers(),
                    userSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        PreCachingRecommender userBasedRecommender = null;
        try {
            Recommender originRecommender = new GenericUserBasedRecommender(trainingDataModel, neighborhood, userSimilarity);
            userBasedRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        /* step 5. evaluate recall rate */
        RecallRateEvaluator recallRateEvaluator = new RecallRateEvaluator();
        L.i("Main", "\n\n******************* itemBased recall rate *****************\n\n");
        new RecallRateRunner(recallRateEvaluator, itemBasedRecommender, totalDataModel, testDataModel).exec();
        L.i("Main", "\n\n******************* userBased recall rate *****************\n\n");
        new RecallRateRunner(recallRateEvaluator, userBasedRecommender, totalDataModel, testDataModel).exec();
        
        /* step 6. evaluate precision rate */
        PrecisionRateEvaluator precisionRateEvaluator = new PrecisionRateEvaluator();
        L.i("Main", "\n\n******************* itemBased precision rate *****************\n\n");
        new PrecisionRateRunner(precisionRateEvaluator, itemBasedRecommender, totalDataModel, testDataModel).exec();
        L.i("Main", "\n\n******************* itemBased precision rate *****************\n\n");
        new PrecisionRateRunner(precisionRateEvaluator, userBasedRecommender, totalDataModel, testDataModel).exec();
        
        /* step 7. evaluate coverage rage */
        CoverageEvaluator coverageEvaluator = new CoverageEvaluator();
        L.i("Main", "\n\n******************* itemBased coverage rate *****************\n\n");
        new CoverageRateRunner(coverageEvaluator, itemBasedRecommender, totalDataModel, testDataModel).exec();
        L.i("Main", "\n\n******************* userBased coverage rate *****************\n\n");
        new CoverageRateRunner(coverageEvaluator, userBasedRecommender, totalDataModel, testDataModel).exec();
        
        /* step 8. evaluate popularity */
        PopularityEvaluator popularityEvaluator = new PopularityEvaluator();
        L.i("Main", "\n\n******************* itemBased popularity *****************\n\n");
        new PopularityRunner(popularityEvaluator, itemBasedRecommender, totalDataModel, testDataModel).exec();
        L.i("Main", "\n\n******************* userBased popularity *****************\n\n");
        new PopularityRunner(popularityEvaluator, userBasedRecommender, totalDataModel, testDataModel).exec();
    }
}
