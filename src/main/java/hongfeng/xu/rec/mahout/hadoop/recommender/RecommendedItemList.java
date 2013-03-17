/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author xuhongfeng
 *
 */
public class RecommendedItemList implements Writable {
    private List<RecommendedItem> items = new ArrayList<RecommendedItem>();
    
    public RecommendedItemList(List<RecommendedItem> items) {
        super();
        this.items = items;
    }

    public RecommendedItemList() {
        super();
    }

    public List<RecommendedItem> getItems() {
        return items;
    }

    public void setItems(List<RecommendedItem> items) {
        this.items = items;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        items.clear();
        int size = in.readInt();
        for (int i=0; i<size; i++) {
            RecommendedItem item = new RecommendedItem();
            item.readFields(in);
            items.add(item);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(items.size());
        for (RecommendedItem item:items) {
            item.write(out);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((items == null) ? 0 : items.hashCode());
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
        RecommendedItemList other = (RecommendedItemList) obj;
        if (items == null) {
            if (other.items != null)
                return false;
        } else if (!items.equals(other.items))
            return false;
        return true;
    }
    
    
}
