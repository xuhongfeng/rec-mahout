/**
 * 2013-3-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.Cache;
import org.apache.mahout.cf.taste.impl.common.Retriever;

/**
 * @author xuhongfeng
 *
 */
public class IDF {
    private final DeliciousDataModel dataModel;

    public IDF(DeliciousDataModel dataModel) {
        super();
        this.dataModel = dataModel;
    }
    
    private Cache<Long, Double> idfCache = new Cache<Long, Double>(new Retriever<Long, Double>() {

        @Override
        public Double get(Long tagId) throws TasteException {
            double d = dataModel.getNumBookmarkIds();
            double dw = dataModel.getNumBookmarkForTag(tagId);
            return Math.log(d/dw);
        }
    });
    public double getIdf(long tagId) throws TasteException {
        return idfCache.get(tagId);
    }
}
