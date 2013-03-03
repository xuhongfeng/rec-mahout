/**
 * 2013-3-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author xuhongfeng
 *
 */
public class TagAllocator {
    private final DataModel bookmarkTagModel;
    
    public TagAllocator(DataModel bookmarkTagModel) {
        super();
        this.bookmarkTagModel = bookmarkTagModel;
    }
    public List<AllocateItem> allocateTagDesc(long tagId) throws TasteException {
        List<AllocateItem> items = allocateTag(tagId);
        Collections.sort(items, COMPARATOR);
        return items;
    }
    
    public List<AllocateItem> allocateTag(long tagId) throws TasteException {
        List<AllocateItem> bookmarkList = allocateBookmark(tagId);
        FastByIDMap<Double> tagMap = new FastByIDMap<Double>();
        for (AllocateItem bookmark:bookmarkList) {
            long bookmarkId = bookmark.getId();
            FastByIDMap<Double> tMap = new FastByIDMap<Double>();
            double total = 0;
            for (Preference pref:bookmarkTagModel.getPreferencesFromUser(bookmarkId)) {
                total += pref.getValue();
                tMap.put(pref.getItemID(), (double) pref.getValue());
            }
            if (total == 0) {
                continue;
            }
            LongPrimitiveIterator tagIdsIterator = tMap.keySetIterator();
            while (tagIdsIterator.hasNext()) {
                long newTagId = tagIdsIterator.nextLong();
                Double value = tagMap.get(newTagId);
                if (value == null) {
                    value = 0.0;
                }
                tagMap.put(newTagId, value + bookmark.getValue()*tMap.get(newTagId)/total);
            }
        }
        return mapToList(tagMap);
    }

    public List<AllocateItem> allocateBookmarkDesc(long tagId) throws TasteException {
        List<AllocateItem> list = allocateBookmark(tagId);
        Collections.sort(list, COMPARATOR);
        return list;
    }
    public List<AllocateItem> allocateBookmark(long tagId) throws TasteException {
        FastByIDMap<Double> bookmarkMap = new FastByIDMap<Double>();
        PreferenceArray prefs = bookmarkTagModel.getPreferencesForItem(tagId);
        double total = 0;
        for (Preference pref:prefs) {
            total += pref.getValue();
            bookmarkMap.put(pref.getUserID(), (double) pref.getValue());
        }
        if (total == 0) {
            return new ArrayList<AllocateItem>();
        }
        LongPrimitiveIterator bookmarkIdsIterator = bookmarkMap.keySetIterator();
        while (bookmarkIdsIterator.hasNext()) {
            long bookmarkId = bookmarkIdsIterator.nextLong();
            bookmarkMap.put(bookmarkId, bookmarkMap.get(bookmarkId)/total);
        }
        return mapToList(bookmarkMap);
    }
    
    private List<AllocateItem> mapToList(FastByIDMap<Double> map) {
        List<AllocateItem> list = new ArrayList<AllocateItem>(map.size());
        for (Map.Entry<Long, Double> entry:map.entrySet()) {
            list.add(new AllocateItem(entry.getKey(), entry.getValue()));
        }
        return list;
    }
    
    public  class AllocateItem {
        private long id;
        private double value;
        
        public AllocateItem(long id, double value) {
            super();
            this.id = id;
            this.value = value;
        }
        public long getId() {
            return id;
        }
        public void setId(long id) {
            this.id = id;
        }
        public double getValue() {
            return value;
        }
        public void setValue(double value) {
            this.value = value;
        }
        @Override
        public String toString() {
            return "AllocateItem [id=" + id + ", value=" + value + "]";
        }
    }
    
    private static final Comparator<AllocateItem> COMPARATOR = new Comparator<AllocateItem>() {
        @Override
        public int compare(AllocateItem o1, AllocateItem o2) {
            if (o1.getValue() > o2.getValue()) {
                return -1;
            }
            return 1;
        }
    };
}
