/**
 * @(#)Main.java, 2013-1-4. 
 * 
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.model.MovielensModel;

import java.io.File;
import java.io.IOException;

/**
 * @author xuhongfeng
 *
 */
public class Main {

    public static void main(String[] args) {
        File dataFile = new File("data/u.data");
        try {
            MovielensModel model = new MovielensModel(dataFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
