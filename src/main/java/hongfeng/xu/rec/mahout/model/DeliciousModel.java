/**
 * 2013-2-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.model;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;

/**
 * @author xuhongfeng
 *
 */
public class DeliciousModel extends FileDataModel {

    private static final long serialVersionUID = -6867259254161762214L;
    
    private static final File DATA_FILE  = new File("data/hetrec2011-delicious-2k/user-bookmark-count.data");

    public DeliciousModel() throws IOException {
        super(DATA_FILE);
    }
    
    

}
