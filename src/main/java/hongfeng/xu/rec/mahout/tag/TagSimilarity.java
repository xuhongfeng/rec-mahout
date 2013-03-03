/**
 * 2013-3-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import hongfeng.xu.rec.mahout.model.DeliciousDataModel;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.similarity.AbstractItemSimilarity;


/**
 * @author xuhongfeng
 *
 */
@Deprecated
public class TagSimilarity extends AbstractItemSimilarity {

    public TagSimilarity(DeliciousDataModel dataModel) {
        super(dataModel.getBookmarkTagModel());
    }

    @Override
    public double itemSimilarity(long tagId1, long tagId2)
            throws TasteException {
        LongPrimitiveIterator bookmarkIdsIterator = getDataModel().getUserIDs();
        double similarity = 0;
        while (bookmarkIdsIterator.hasNext()) {
            long bookmarkId = bookmarkIdsIterator.nextLong();
            FastIDSet tagIdSet = getDataModel().getItemIDsFromUser(bookmarkId);
            if (tagIdSet.contains(tagId1) && tagIdSet.contains(tagId2)) {
                similarity++;
            }
        }
        return similarity==0?Double.NaN:similarity;
    }

    @Override
    public double[] itemSimilarities(long itemID1, long[] itemID2s)
            throws TasteException {
        int length = itemID2s.length;
        double[] result = new double[length];
        for (int i = 0; i < length; i++) {
          result[i] = itemSimilarity(itemID1, itemID2s[i]);
        }
        return result;
    }

}