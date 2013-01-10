/**
 * 2013-1-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.model.MovielensModel;
import hongfeng.xu.rec.mahout.recommender.movielens.PreCachingRecommender;
import hongfeng.xu.rec.mahout.runner.AbsHitRateRunner;
import hongfeng.xu.rec.mahout.runner.movielens.PrecisionRateRunner;
import hongfeng.xu.rec.mahout.runner.movielens.RecallRateRunner;
import hongfeng.xu.rec.mahout.util.DataModelUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
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
                    DataModelUtils.split(totalDataModel, 1, 0.8);
            trainingDataModel = models.getFirst();
            testDataModel = models.getSecond();
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        /* step 3.  build similarity */
        CachingItemSimilarity similarity = null;
        try {
            similarity = new CachingItemSimilarity(
                    new PearsonCorrelationSimilarity(trainingDataModel)
                    , trainingDataModel);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        /* step 4. build Recommender */
        L.i("Main", "build recommender");
        PreCachingRecommender recommender = null;
        try {
            Recommender originRecommender= new GenericItemBasedRecommender(trainingDataModel,
                    similarity);
//            Recommender originRecommender = new KnnItemBasedRecommender(trainingDataModel,
//                    similarity, new NonNegativeQuadraticOptimizer(), 20);
            recommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        /* step 5. evaluate recall rate */
        L.i("Main", "evaluate recall rate");
        RecallRateEvaluator recallRateEvaluator = new RecallRateEvaluator();
        new RecallRateRunner(recallRateEvaluator, recommender, testDataModel).exec();
        
        /* step 6. evaluate precision rate */
        L.i("Main", "evaluate precision rate");
        PrecisionRateEvaluator precisionRateEvaluator = new PrecisionRateEvaluator();
        new PrecisionRateRunner(precisionRateEvaluator, recommender, testDataModel).exec();
    }
}
