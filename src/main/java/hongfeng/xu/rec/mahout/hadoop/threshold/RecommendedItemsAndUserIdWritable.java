/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendedItem;
import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendedItemList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author xuhongfeng
 *
 */
public class RecommendedItemsAndUserIdWritable implements Writable {
    private RecommendedItemList items = new RecommendedItemList();
    private int userId;

    public RecommendedItemsAndUserIdWritable() {
        super();
    }
    
    public RecommendedItemsAndUserIdWritable(int userId, List<RecommendedItem> list) {
        this.userId = userId;
        items.setItems(list);
    }
    
    public List<RecommendedItem> getItems() {
        return items.getItems();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userId);
        items.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readInt();
        items.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((items == null) ? 0 : items.hashCode());
        result = prime * result + userId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecommendedItemsAndUserIdWritable other = (RecommendedItemsAndUserIdWritable) obj;
        if (items == null) {
            if (other.items != null)
                return false;
        } else if (!items.equals(other.items))
            return false;
        if (userId != other.userId)
            return false;
        return true;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setItems(RecommendedItemList items) {
        this.items = items;
    }
    
}
