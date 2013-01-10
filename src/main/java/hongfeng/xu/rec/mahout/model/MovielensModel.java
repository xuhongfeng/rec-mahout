/**
 * 2013-1-4
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
public class MovielensModel extends FileDataModel {
    private static final long serialVersionUID = 1647594091122073867L;

    /**
     * @param dataFile
     * @throws IOException
     */
    public MovielensModel(File dataFile) throws IOException {
        super(dataFile, false, Long.MAX_VALUE);
    }


}
