/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataLine;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataLineArray;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel.RawDataSet;

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
        
    private static Random random = RandomUtils.getRandom();
    
    public static Pair<DataModel, DataModel> split(DataModel totalModel,
            double totalPercentage, double trainingPercentage) throws TasteException {
        
        random = RandomUtils.getRandom();

        int numUsers = totalModel.getNumUsers();
        FastByIDMap<PreferenceArray> trainingPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (totalPercentage* numUsers));
        FastByIDMap<PreferenceArray> testPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (totalPercentage* numUsers));

        LongPrimitiveIterator it = totalModel.getUserIDs();

        while (it.hasNext()) {
            long userID = it.nextLong();
            if (random.nextDouble() < totalPercentage) {
                splitOneUsersPrefs(trainingPercentage, trainingPrefs,
                        testPrefs, userID, totalModel);
            }
        }

        DataModel trainingModel = new GenericDataModel(trainingPrefs);
        DataModel testModel = new GenericDataModel(testPrefs);
        
        return new Pair<DataModel, DataModel>(trainingModel, testModel);
    }
    private static void splitOneUsersPrefs(double trainingPercentage,
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
    public static Pair<DeliciousDataModel, DeliciousDataModel> splitDeliciousDataModel(DeliciousDataModel totalModel,
            double totalPercentage, double trainingPercentage) throws TasteException {
        random = RandomUtils.getRandom();
        
        RawDataSet totalRawDataSet = totalModel.getRawDataSet();
        
        RawDataSet newTotalRawDataSet = new RawDataSet();
        for (RawDataLineArray array:totalRawDataSet.getArrays()) {
            if (random.nextDouble() <= totalPercentage) {
                newTotalRawDataSet.add(array);
            }
        }
        Pair<RawDataSet, RawDataSet> rawDataSetPair = splitRawDataSet(newTotalRawDataSet, trainingPercentage);
        DeliciousDataModel trainingDataModel = new DeliciousDataModel(rawDataSetPair.getFirst());
        DeliciousDataModel testDataModel = new DeliciousDataModel(rawDataSetPair.getSecond());
        return new Pair<DeliciousDataModel, DeliciousDataModel>(trainingDataModel, testDataModel);
    }
    
    private static Pair<RawDataSet, RawDataSet> splitRawDataSet(RawDataSet totalRawDataSet
            , double trainingPercentage) {
        RawDataSet trainingDataSet = new RawDataSet();
        RawDataSet testDataSet = new RawDataSet();
        for (RawDataLineArray array:totalRawDataSet.getArrays()) {
            Pair<RawDataLineArray, RawDataLineArray> arrayPair = splitRawDataArray(array, trainingPercentage);
            trainingDataSet.add(arrayPair.getFirst());
            testDataSet.add(arrayPair.getSecond());
        }
        return new Pair<RawDataSet, RawDataSet>(trainingDataSet, testDataSet);
    }
    
    private static Pair<RawDataLineArray, RawDataLineArray> splitRawDataArray(RawDataLineArray array, double trainingPercentage) {
        RawDataLineArray trainingArray = new RawDataLineArray();
        RawDataLineArray testArray = new RawDataLineArray();
        for (RawDataLine line:array) {
            if (random.nextDouble() <= trainingPercentage) {
                trainingArray.add(line);
            } else {
                testArray.add(line);
            }
        }
        return new Pair<RawDataLineArray, RawDataLineArray>(trainingArray, testArray);
    }
}