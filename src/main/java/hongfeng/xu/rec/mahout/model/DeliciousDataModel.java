/**
 * 2013-2-28
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author xuhongfeng
 *
 */
public class DeliciousDataModel extends AbstractDataModel {
    private static final long serialVersionUID = -3718120174918137829L;
    
    private DataModel userBookmark;
    private DataModel userTag;
    private DataModel bookmarkTag;
    
    private RawDataSet rawDataSet;

    public DeliciousDataModel(File file) throws IOException {
        super();
        rawDataSet = RawDataSet.parse(file);
        init(rawDataSet);
    }
    
    public DeliciousDataModel(RawDataSet rawDataSet) {
        super();
        init(rawDataSet);
    }
    
    private void init(RawDataSet rawDataSet) {
        this.rawDataSet = rawDataSet;
        
        DataModel[] models = rawDataSet.calculateDataModel();
        userBookmark = models[0];
        userTag = models[1];
        bookmarkTag = models[2];
    }
    
    /** get or set DataModel **/
    
    public RawDataSet getRawDataSet() {
        return rawDataSet;
    }
    
    public DataModel getUserTagModel() {
        return userTag;
    }
    
    public void setUserTagModel(DataModel model) {
        this.userTag = model;
        this.rawDataSet = null;
    }
    
    public DataModel getBookmarkTagModel() {
        return bookmarkTag;
    }
    
    /** get preference **/
    
    public Float getUserBookmarkValue(long userID, long bookmarkID) throws TasteException {
        try {
            return userBookmark.getPreferenceValue(userID, bookmarkID);
        } catch (NoSuchUserException e) {
            return null;
        }
    }
    
    public Float getUserTagValue(long userID, long tagID) throws TasteException {
        try {
            return getUserTagModel().getPreferenceValue(userID, tagID);
        } catch (NoSuchUserException e) {
            return null;
        }
    }
    
    public Float getBookmarkTagValue(long bookmarkID, long tagID) throws TasteException {
        try {
            return getBookmarkTagModel().getPreferenceValue(bookmarkID, tagID);
        } catch (NoSuchUserException e) {
            return null;
        }
    }
    
    /** get preference array **/
    
    public PreferenceArray getBookmarkTagPrefArray(long bookmarkId) throws TasteException {
        try {
            return getBookmarkTagModel().getPreferencesFromUser(bookmarkId);
        } catch (NoSuchUserException e) {
            return new GenericUserPreferenceArray(0);
        }
    }
    
    public PreferenceArray getUserTagPrefArray(long userID) throws TasteException {
        try {
            return getUserTagModel().getPreferencesFromUser(userID);
        } catch (NoSuchUserException e) {
            return new GenericUserPreferenceArray(0);
        }
    }
    
    /** getIds **/
    
    public LongPrimitiveIterator getBookmarkIds() throws TasteException {
        return getItemIDs();
    }
    
    public LongPrimitiveIterator getTagIds() throws TasteException {
        return userTag.getItemIDs();
    }
    
    /** get size  **/
    public int getNumBookmarkIds() throws TasteException {
        return getNumItems();
    }
    
    public int getNumBookmarkForTag(long tagId) throws TasteException {
        return getBookmarkTagModel().getNumUsersWithPreferenceFor(tagId);
    }
    
    public int getNumTagIds() throws TasteException {
        return getBookmarkTagModel().getNumItems();
    }
    
    /** delegate **/

    @Override
    public LongPrimitiveIterator getUserIDs() throws TasteException {
        return userBookmark.getUserIDs();
    }

    @Override
    public PreferenceArray getPreferencesFromUser(long userID)
            throws TasteException {
        return userBookmark.getPreferencesFromUser(userID);
    }

    @Override
    public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
        return userBookmark.getItemIDsFromUser(userID);
    }

    @Override
    public LongPrimitiveIterator getItemIDs() throws TasteException {
        return userBookmark.getItemIDs();
    }

    @Override
    public PreferenceArray getPreferencesForItem(long itemID)
            throws TasteException {
        return userBookmark.getPreferencesForItem(itemID);
    }

    @Override
    public Float getPreferenceValue(long userID, long itemID)
            throws TasteException {
        return getUserBookmarkValue(userID, itemID);
    }

    @Override
    public Long getPreferenceTime(long userID, long itemID)
            throws TasteException {
        return userBookmark.getPreferenceTime(userID, itemID);
    }

    @Override
    public int getNumItems() throws TasteException {
        return userBookmark.getNumItems();
    }

    @Override
    public int getNumUsers() throws TasteException {
        return userBookmark.getNumUsers();
    }

    @Override
    public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
        return userBookmark.getNumUsersWithPreferenceFor(itemID);
    }

    @Override
    public int getNumUsersWithPreferenceFor(long itemID1, long itemID2)
            throws TasteException {
        return userBookmark.getNumUsersWithPreferenceFor(itemID1, itemID2);
    }

    @Override
    public void setPreference(long userID, long itemID, float value)
            throws TasteException {
        userBookmark.setPreference(userID, itemID, value);
    }

    @Override
    public void removePreference(long userID, long itemID)
            throws TasteException {
        userBookmark.removePreference(userID, itemID);
    }

    @Override
    public boolean hasPreferenceValues() {
        return userBookmark.hasPreferenceValues();
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        userBookmark.refresh(alreadyRefreshed);
    }

    public static class RawDataSet {
        private final FastByIDMap<RawDataLineArray> map = new FastByIDMap<RawDataLineArray>();
        
        public List<RawDataLineArray> getArrays() {
            List<RawDataLineArray> list = new ArrayList<RawDataLineArray>(arrayCount());
            for (Map.Entry<Long, RawDataLineArray> entry:map.entrySet()) {
                list.add(entry.getValue());
            }
            return list;
        }
        
        public int arrayCount() {
            return map.size();
        }
        
        public RawDataLineArray get(long userId) {
            return map.get(userId);
        }
        
        public void add(RawDataLineArray array) {
            if (array.size() > 0) {
                map.put(array.getUserId(), array);
            }
        }
        
        public void add(RawDataLine line) {
            RawDataLineArray array = map.get(line.userId);
            if (array == null) {
                array = new RawDataLineArray();
                map.put(line.userId, array);
            }
            array.add(line);
        }
        
        public int lineCount() {
            int count = 0;
            for (Map.Entry<Long, RawDataLineArray> entry:map.entrySet()) {
                RawDataLineArray array = entry.getValue();
                count += array.size();
            }
            return count;
        }
        
        public LongPrimitiveIterator userIdIterator() {
            return map.keySetIterator();
        }
        
        public Iterator<RawDataLine> lineIterator() {
            return new RawDataLineIterator(this);
        }
        
        public DataModel[] calculateDataModel() {
            PrefItemMap userBookmarkMap = new PrefItemMap();
            PrefItemMap userTagMap = new PrefItemMap();
            PrefItemMap bookmarkTagMap = new PrefItemMap();
            
            Iterator<Long> userIdIterator = userIdIterator();
            while (userIdIterator.hasNext()) {
                long userId = userIdIterator.next();
                for (RawDataLine line:get(userId)) {
                    userBookmarkMap.addOrIncr(line.userId, line.bookmarkId);
                    userTagMap.addOrIncr(line.userId, line.tagId);
                    bookmarkTagMap.addOrIncr(line.bookmarkId, line.tagId);
                }
            }
            
            DataModel[] array = new DataModel[3];
            array[0] = userBookmarkMap.calculateDataModel();
            array[1] = userTagMap.calculateDataModel();
            array[2] = bookmarkTagMap.calculateDataModel();
            return array;
        }
        
        public static RawDataSet parse(File file) throws IOException {
            RawDataSet set = new RawDataSet();
            List<String> lines = FileUtils.readLines(file);
            for (String line:lines) {
                set.add(RawDataLine.parse(line));
            }
            return set;
        }
    }
    
    public static class RawDataLineArray implements Iterable<RawDataLine> {
        private final List<RawDataLine> list = new ArrayList<RawDataLine>();
        
        public long getUserId() {
            return list.get(0).userId;
        }
        
        public void add(RawDataLine line) {
            list.add(line);
        }
        
        @Override
        public Iterator<RawDataLine> iterator() {
            return list.iterator();
        }
        
        public int size() {
            return list.size();
        }
    }
    
    public static class RawDataLine {
        public final long userId;
        public final long bookmarkId;
        public final long tagId;
        
        public RawDataLine(long userId, long bookmarkId, long tagId) {
            super();
            this.userId = userId;
            this.bookmarkId = bookmarkId;
            this.tagId = tagId;
        }
        
        public static RawDataLine parse(String line) {
            String[] ss = line.split("\\s");
            long userId = Long.valueOf(ss[0]);
            long bookmarkId = Long.valueOf(ss[2]);
            long tagId = Long.valueOf(ss[1]);
//            long bookmarkId = Long.valueOf(ss[1]);
//            long tagId = Long.valueOf(ss[2]);
            return new RawDataLine(userId, bookmarkId, tagId);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (bookmarkId ^ (bookmarkId >>> 32));
            result = prime * result + (int) (tagId ^ (tagId >>> 32));
            result = prime * result + (int) (userId ^ (userId >>> 32));
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
            RawDataLine other = (RawDataLine) obj;
            if (bookmarkId != other.bookmarkId)
                return false;
            if (tagId != other.tagId)
                return false;
            if (userId != other.userId)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "RawDataLine [userId=" + userId + ", bookmarkId="
                    + bookmarkId + ", tagId=" + tagId + "]";
        }
        
    }
    
    /**
     * 
     * [id1, id2, count]
     * @author xuhongfeng
     *
     */
    public static class PrefItemArray {
        private long id1;
        private final FastByIDMap<PrefItem> map = new FastByIDMap<PrefItem>();
        
        public void addOrIncr(long id1, long id2) {
            if (map.size() == 0) {
                this.id1 = id1;
            } else if (this.id1 != id1){
                throw new IllegalArgumentException();
            }
            PrefItem item = map.get(id2);
            if (item == null) {
                item = new PrefItem(id1, id2);
                map.put(id2, item);
            }
            item.incr();
        }
        
        public PreferenceArray calculatePreferenceArray() {
            List<Preference> prefs = new ArrayList<Preference>();
            for (Map.Entry<Long, PrefItem> entry:map.entrySet()) {
                PrefItem item = entry.getValue();
                prefs.add(item.toPreference());
            }
            return new GenericUserPreferenceArray(prefs);
        }
    }
    
    /**
     * map<id1, [id1, id2, count]>
     * @author xuhongfeng
     *
     */
    public static class PrefItemMap {
        private final FastByIDMap<PrefItemArray> map = new FastByIDMap<PrefItemArray>();
        
        public void addOrIncr(long id1, long id2) {
            PrefItemArray array = map.get(id1);
            if (array == null) {
                array = new PrefItemArray();
                map.put(id1, array);
            }
            array.addOrIncr(id1, id2);
        }
        
        public DataModel calculateDataModel() {
            FastByIDMap<PreferenceArray> tMap = new FastByIDMap<PreferenceArray>();
            Iterator<Long> it = map.keySetIterator();
            while (it.hasNext()) {
                long id1 = it.next();
                tMap.put(id1, this.map.get(id1).calculatePreferenceArray());
            }
            return new GenericDataModel(tMap);
        }
    }
    
    public static class PrefItem {
        public final long id1;
        public final long id2;
        private int count;
        
        public PrefItem(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }
        
        public void incr() {
            count++;
        }
        
        @Override
        public int hashCode() {
            return hash(id1, id2);
        }
        
        public static int hash(long id1, long id2) {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (id1 ^ (id1 >>> 32));
            result = prime * result + (int) (id2 ^ (id2 >>> 32));
            return result;
        }
        
        public Preference toPreference() {
            return new GenericPreference(id1, id2, count);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PrefItem other = (PrefItem) obj;
            if (id1 != other.id1)
                return false;
            if (id2 != other.id2)
                return false;
            return true;
        }

        public int getCount() {
            return count;
        }
        public void setCount(int count) {
            this.count = count;
        }
    }
    
    private static class RawDataLineIterator implements Iterator<RawDataLine> {
        private final RawDataSet rawDataSet;
        private LongPrimitiveIterator userIdIterator;
        private Iterator<RawDataLine> lineIterator;

        public RawDataLineIterator(RawDataSet rawDataSet) {
            super();
            this.rawDataSet = rawDataSet;
            userIdIterator = rawDataSet.userIdIterator();
        }

        @Override
        public boolean hasNext() {
            if (lineIterator == null || !lineIterator.hasNext()) {
                if (userIdIterator.hasNext()) {
                    long userId = userIdIterator.next();
                    RawDataLineArray array = rawDataSet.get(userId);
                    lineIterator = array.iterator();
                    return hasNext();
                } else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RawDataLine next() {
            if (lineIterator==null || !lineIterator.hasNext()) {
                long userId = userIdIterator.next();
                RawDataLineArray array = rawDataSet.get(userId);
                lineIterator = array.iterator();
                return next();
            }
            return lineIterator.next();
        }

        @Override
        public void remove() {
            lineIterator.remove();
        }
    }
}
