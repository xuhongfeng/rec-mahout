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

    public DeliciousModel(File dataFile, boolean transpose,
            long minReloadIntervalMS) throws IOException {
        super(dataFile, transpose, minReloadIntervalMS);
    }

    public DeliciousModel(File dataFile) throws IOException {
        super(dataFile);
    }
    
    

}
