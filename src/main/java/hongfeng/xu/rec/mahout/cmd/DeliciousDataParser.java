/**
 * 2013-2-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

/**
 * @author xuhongfeng
 *
 */
public class DeliciousDataParser {
    
    private static final String DIR = "data/hetrec2011-delicious-2k";
    private static final String INPUT_PATH = DIR + "/user_taggedbookmarks-timestamps.dat";
    private static final String OUTPUT_USER_BOOKMARK = DIR + "/user-bookmark-count.data";
    private static final String OUTPUT_BOOKMARK_TAG = DIR + "/bookmark-tag-count.data";
    private static final String OUTPUT_USER_TAG = DIR + "/user-tag-count.data";
    
    public void parse() throws IOException {
        File input = new File(INPUT_PATH);
        File outputUserBookmark = new File(OUTPUT_USER_BOOKMARK);
        File outputBookmarkTag = new File(OUTPUT_BOOKMARK_TAG);
        File outputUserTag = new File(OUTPUT_USER_TAG);
        
        /** read file **/
        List<String> lines = FileUtils.readLines(input);
        //first line is table head
        lines.remove(0);
        L.i(this, "input lines count = " + lines.size());
        
        ItemSet userBookmarkSet = new ItemSet();
        ItemSet userTagSet = new ItemSet();
        ItemSet bookmarkTagSet = new ItemSet();
        for (String line:lines) {
            String[] ss = line.split("\\s");
            int uid = Integer.valueOf(ss[0]);
            int bookmarkId = Integer.valueOf(ss[1]);
            int tagId = Integer.valueOf(ss[2]);
            userBookmarkSet.add(uid, bookmarkId);
            userTagSet.add(uid, tagId);
            bookmarkTagSet.add(bookmarkId, tagId);
        }
        lines.clear();
        lines = null;
        L.i(this, "user-bookmark-set size = " + userBookmarkSet.size() + ", total count = " + userBookmarkSet.totalCount());
        L.i(this, "user-tag-set size = " + userTagSet.size() + ", total count = " + userTagSet.totalCount());
        L.i(this, "bookmark-tag-set size = " + bookmarkTagSet.size() + ", total count = " + bookmarkTagSet.totalCount());
        
        /** write file **/
        FileUtils.writeLines(outputUserBookmark, userBookmarkSet.toLines());
        FileUtils.writeLines(outputUserTag, userTagSet.toLines());
        FileUtils.writeLines(outputBookmarkTag, bookmarkTagSet.toLines());
    }
    
    /**
     * 
     * @author xuhongfeng
     *
     */
    private static class ItemSet implements Iterable<Item> {
        private final Map<Integer, Item> map = new HashMap<Integer, Item>();
        
        public void add(int id1, int id2) {
            int hashCode = hash(id1, id2);
            Item item = map.get(hashCode);
            if (item == null) {
                item = new Item(id1, id2);
                map.put(hashCode, item);
            }
            item.incrCount();
        }
        
        public int totalCount() {
            int count = 0;
            for (Item item:this) {
                count += item.getCount();
            }
            return count;
        }
        
        private int hash(int id1, int id2) {
            int prime = 31;
            int result = 1;
            result = result*prime + id1;
            result = result*prime + id2;
            return result;
        }
        
        public List<String> toLines() {
            List<String> lines = new ArrayList<String>(size());
            for (Item item:this) {
                lines.add(item.toLine());
            }
            return lines;
        }
        
        public int size() {
            return map.size();
        }

        @Override
        public Iterator<Item> iterator() {
            return map.values().iterator();
        }
    }
    
    /**
     *  [id1, id2, count]
     * 
     * @author xuhongfeng
     *
     */
    private static class Item {
        public final int id1;
        public final int id2;
        private int count;
        
        public Item(int id1, int id2) {
            super();
            this.id1 = id1;
            this.id2 = id2;
            this.count = 0;
        }
        
        public String toLine() {
            return String.format("%d\t%d\t%d", id1, id2, count);
        }
        
        public void incrCount() {
            count++;
        }
        
        public int getCount() {
            return count;
        }
        
    }
    
    public static void main(String[] args) {
        try {
            new DeliciousDataParser().parse();
        } catch (IOException e) {
            L.e("DeliciousDataParser", e);
        }
    }
    
    private static void testItemSet() {
        ItemSet set = new ItemSet();
        set.add(1, 3);
        set.add(1, 3);
        L.i("DeliciousDataParser", "item set size = " + set.size());
        L.i("DeliciousDataParser", "item count = " + set.iterator().next().getCount());
    }
}