/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import java.util.List;
import java.util.Random;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;

import com.google.common.collect.Lists;

/**
 * @author xuhongfeng
 *
 */
public class DataModelUtils {
    
    public static Pair<DataModel, DataModel> split(DataModel totalModel,
            double totalPercentage, double trainingPercentage) throws TasteException {

        int numUsers = totalModel.getNumUsers();
        FastByIDMap<PreferenceArray> trainingPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (totalPercentage* numUsers));
        FastByIDMap<PreferenceArray> testPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (totalPercentage* numUsers));

        LongPrimitiveIterator it = totalModel.getUserIDs();
        
        Random random = RandomUtils.getRandom();

        while (it.hasNext()) {
            long userID = it.nextLong();
            if (random.nextDouble() < totalPercentage) {
                splitOneUsersPrefs(trainingPercentage, trainingPrefs,
                        testPrefs, userID, totalModel, random);
            }
        }

        DataModel trainingModel = new GenericDataModel(trainingPrefs);
        DataModel testModel = new GenericDataModel(testPrefs);
        
        return new Pair<DataModel, DataModel>(trainingModel, testModel);
    }
    private static void splitOneUsersPrefs(double trainingPercentage,
            FastByIDMap<PreferenceArray> trainingPrefs,
            FastByIDMap<PreferenceArray> testPrefs, long userID,
            DataModel dataModel, Random random) throws TasteException {
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
