/**
 * 2013-1-6 xuhongfeng
 */
package hongfeng.xu.rec.mahout.eval;

import java.util.List;
import java.util.Random;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.RandomUtils;

import com.google.common.collect.Lists;

/**
 * @author xuhongfeng
 */
public abstract class AbsTopNEvaluator {

    private final Random random;

    public AbsTopNEvaluator() {
        random = RandomUtils.getRandom();
    }

    public double evaluate(RecommenderBuilder recommenderBuilder,
            DataModel dataModel, double trainingPercentage, double evaluationPercentage, int N)
            throws TasteException {

        int numUsers = dataModel.getNumUsers();
        FastByIDMap<PreferenceArray> trainingPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (evaluationPercentage * numUsers));
        FastByIDMap<PreferenceArray> testPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (evaluationPercentage * numUsers));

        LongPrimitiveIterator it = dataModel.getUserIDs();

        while (it.hasNext()) {
            long userID = it.nextLong();
            if (random.nextDouble() < evaluationPercentage) {
                splitOneUsersPrefs(trainingPercentage, trainingPrefs,
                        testPrefs, userID, dataModel);
            }
        }

        DataModel trainingModel = new GenericDataModel(trainingPrefs);
        DataModel testModel = new GenericDataModel(testPrefs);
        Recommender recommender = recommenderBuilder
                .buildRecommender(trainingModel);
        return evaluate(recommender, testModel, N);
    }

    protected abstract double evaluate(Recommender recommender,
            DataModel testModel, int N) throws TasteException;

    private void splitOneUsersPrefs(double trainingPercentage,
            FastByIDMap<PreferenceArray> trainingPrefs,
            FastByIDMap<PreferenceArray> testPrefs, long userID,
            DataModel dataModel) throws TasteException {
        List<Preference> oneUserTrainingPrefs = null;
        List<Preference> oneUserTestPrefs = null;
        PreferenceArray prefs = dataModel.getPreferencesFromUser(userID);
        int size = prefs.length();
        for (int i = 0; i < size; i++) {
            Preference newPref = new GenericPreference(userID,
                    prefs.getItemID(i), prefs.getValue(i));
            if (random.nextDouble() < trainingPercentage) {
                if (oneUserTrainingPrefs == null) {
                    oneUserTrainingPrefs = Lists.newArrayListWithCapacity(3);
                }
                oneUserTrainingPrefs.add(newPref);
            } else {
                if (oneUserTestPrefs == null) {
                    oneUserTestPrefs = Lists.newArrayListWithCapacity(3);
                }
                oneUserTestPrefs.add(newPref);
            }
        }
        if (oneUserTrainingPrefs != null) {
            trainingPrefs.put(userID, new GenericUserPreferenceArray(
                    oneUserTrainingPrefs));
            if (oneUserTestPrefs != null) {
                testPrefs.put(userID, new GenericUserPreferenceArray(
                        oneUserTestPrefs));
            }
        }
    }
}
