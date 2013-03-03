/**
 * 2013-3-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.model.DeliciousDataModel;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xuhongfeng
 *
 */
public class TestTagFlatter {
    private TagFlatter flatter;
    private DeliciousDataModel dataModel;

    @BeforeClass
    public void setUp() {
        try {
            dataModel = new DeliciousDataModel(DeliciousDataConfig.RAW_DATA_FILE);
            flatter = new TagFlatter();
        } catch (IOException e) {
            Assert.fail("", e);
        }
    }
    
    @Test
    public void test() {
        try {
            DataModel oldUserTag = dataModel.getUserTagModel();
            DataModel newUserTag = flatter.flat(dataModel).getUserTagModel();
            
            long tagId = 1631;
            long userId = 1263;
            printPrefArray(oldUserTag.getPreferencesFromUser(userId));
            printPrefArray(newUserTag.getPreferencesFromUser(userId));
        } catch (TasteException e) {
            Assert.fail("", e);
        }
    }
    
    private void printPrefArray(PreferenceArray prefs) {
        StringBuilder sb = new StringBuilder();
        for (Preference pref:prefs) {
            sb.append(pref.getItemID() + ":" + pref.getValue() + "\t");
        }
        L.i(this, sb.toString());
    }
}
