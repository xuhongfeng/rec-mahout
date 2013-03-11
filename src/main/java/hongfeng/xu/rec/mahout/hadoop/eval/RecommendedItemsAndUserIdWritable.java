/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;

/**
 * @author xuhongfeng
 *
 */
public class RecommendedItemsAndUserIdWritable extends VarLongWritable {
    private RecommendedItemsWritable items;

    public RecommendedItemsAndUserIdWritable() {
        super();
    }
    
    public RecommendedItemsAndUserIdWritable(long itemId, List<RecommendedItem> list) {
        super(itemId);
        items = new RecommendedItemsWritable(list);
    }
    
    public List<RecommendedItem> getItems() {
        return items.getRecommendedItems();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        items.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        items = new RecommendedItemsWritable();
        items.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((items == null) ? 0 : items.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecommendedItemsAndUserIdWritable other = (RecommendedItemsAndUserIdWritable) obj;
        if (items == null) {
            if (other.items != null)
                return false;
        } else if (!items.equals(other.items))
            return false;
        return true;
    }
    
    @Override
    public RecommendedItemsAndUserIdWritable clone() {
        return new RecommendedItemsAndUserIdWritable(get(), items.getRecommendedItems());
    }
}
