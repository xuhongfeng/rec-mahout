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
//    private static final String DATA_FILE = "data/movielens-1m/ratings.dat";
    private static final File DATA_FILE = new File("data/movielens-100k/u.data");

    /**
     * @param dataFile
     * @throws IOException
     */
    public MovielensModel() throws IOException {
        super(DATA_FILE, false, Long.MAX_VALUE);
    }


}
