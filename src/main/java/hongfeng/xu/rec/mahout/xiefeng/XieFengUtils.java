/**
 * 2013-1-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.xiefeng;

import org.apache.commons.math.complex.ComplexField;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author xuhongfeng
 *
 */
public class XieFengUtils {

    public ComplexMatrix parseUserItemMatrix(DataModel dataModel) throws TasteException {
        int numUsers = dataModel.getNumUsers();
        int numItems = dataModel.getNumItems();
        int matixDimention = numUsers + numItems;
        ComplexMatrix matrix = new ComplexMatrix(ComplexField.getInstance(), matixDimention, matixDimention);
        
//        PreferenceArray preferenceArray = null;
//        LongPrimitiveIterator uit = dataModel.getUserIDs();
//        long userId, itemId
//        int i, row, column = 0;
//        double value = 0;
//        Preference preference;
//        while (uit.hasNext()) {
//            userId = uit.nextLong();
//            preferenceArray = dataModel.getPreferencesFromUser(userId);
//            for (i=0; i<preferenceArray.length(); i++) {
//                preference = preferenceArray.get(i);
//                itemId = preference.getItemID();
//                row = userId;
//                column = numUsers + 
//                matrix.setEntry(row, column, preference.getValue());
//            }
//        }
//        
        return matrix;
    }
}
