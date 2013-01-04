/**
 * @(#)Main.java, 2013-1-4. 
 * 
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.model.MovielensModel;
import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 * @author xuhongfeng
 *
 */
public class Main {
    public static void main(String[] args) {
        File dataFile = new File("data/u.data");
        try {
            MovielensModel model = new MovielensModel(dataFile);
            ItemSimilarity similarity;
            try {
                similarity = new PearsonCorrelationSimilarity(model);
            } catch (TasteException e) {
                L.e("Main", e);
                return;
            }
            GenericItemBasedRecommender recomender = new GenericItemBasedRecommender(model, similarity);
        } catch (IOException e) {
            L.e("Main", e);
        }
    }
}
