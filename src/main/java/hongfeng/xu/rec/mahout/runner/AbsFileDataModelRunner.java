/**
 * 2013-1-8 xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;

/**
 * @author xuhongfeng
 */
public abstract class AbsFileDataModelRunner implements Runner {
    @Override
    public final void exec() {
        String filePath = getFilePath();
        File file = new File(filePath);
        FileDataModel dataModel;
        try {
            dataModel = createModel(file);
        } catch (IOException e) {
            L.e(this, e);
            return;
        }
        innerExec(dataModel);
    }

    protected abstract void innerExec(FileDataModel dataModel);
    protected abstract FileDataModel createModel(File file) throws IOException;
    protected abstract String getFilePath();
}
