/**
 * 2013-3-25
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.misc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

/**
 * @author xuhongfeng
 *
 */
public class FilterMovielens10M {
    
    public static void main(String[] args) {
//        File input = new File("data/hetrec2011-movielens-2k/user_taggedmovies-timestamps.dat");
//        File output = new File("data/hetrec2011-movielens-2k/user-record-count.dat");
        File input = new File("data/movielens-10m/user-tag-timestamp.dat");
        File output = new File("data/movielens-10m/user-record-count.dat");
        File filteredFile = new File("data/movielens-10m/final-user-item-tag-timestamp.dat");
        
        try {
            List<Line> lines = Line.parse(input);
            Map<Long, Item> map = new HashMap<Long, Item>();
            for (Line line:lines) {
                Item item = map.get(line.userId);
                if (item == null) {
                    item = new Item(line.userId);
                    map.put(item.userId, item);
                }
                item.add(line.itemId);
            }
            
            List<Item> list = new ArrayList<Item>(map.size());
            list.addAll(map.values());
            Collections.sort(list, new Comparator<Item>() {
                @Override
                public int compare(Item o1, Item o2) {
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    }
                    if (o1.getCount() < o2.getCount()) {
                        return 1;
                    }
                    return 0;
                }
            });
            
            List<String> strLines = new ArrayList<String>(list.size());
            for (Item item:list) {
                strLines.add(String.format("%d\t%d", item.userId, item.getCount()));
            }
            
            if (output.exists()) {
                output.delete();
            }
            FileUtils.writeLines(output, strLines);
            
            int n1=0, n2=0, n3=0;
            int TOP = 1000, BOTTOM = 20;
            for (Item item:list) {
                if (item.getCount() > TOP) {
                    n1++;
                } else if (item.getCount() < BOTTOM) {
                    n3++;
                } else {
                    n2++;
                }
            }
            System.out.println("count > " + TOP + " " + n1);
            System.out.println(BOTTOM + "<= count<" + TOP + " " + n2);
            System.out.println("count < " + BOTTOM + " " + n3);
            
            
            //filter
            
            List<Line> newLines = new ArrayList<Line>();
            for (Line line:lines) {
                Item item = map.get(line.userId);
                if (item.getCount()>=BOTTOM && item.getCount()<=TOP) {
                    newLines.add(line);
                }
            }
            System.out.println("filter lines.size = " + newLines.size());
            
            Collections.sort(newLines, new Comparator<Line>() {
                @Override
                public int compare(Line o1, Line o2) {
                    if (o1.userId < o2.userId) {
                        return -1;
                    }
                    if (o1.userId > o2.userId) {
                        return 1;
                    }
                    if (o1.timestamp < o2.timestamp) {
                        return -1;
                    }
                    if (o1.timestamp > o2.timestamp) {
                        return 1;
                    }
                    if (o1.itemId < o2.itemId) {
                        return -1;
                    }
                    if (o1.itemId > o2.itemId) {
                        return 1;
                    }
                    if (o1.tagId < o2.tagId) {
                        return -1;
                    }
                    if (o1.tagId > o2.tagId) {
                        return 1;
                    }
                    return 0;
                }
            });
            
            strLines.clear();
            for (Line line:newLines) {
                strLines.add(line.toString());
            }
            if (filteredFile.exists()) {
                filteredFile.delete();
            }
            FileUtils.writeLines(filteredFile, strLines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static class Item {
        public final long userId;
        private final Set<Long> items = new HashSet<Long>();
        private int count = -1;

        public Item(long userId) {
            super();
            this.userId = userId;
        }
        
        public int getCount() {
            if (count == -1) {
                count = items.size();
            }
            return count;
        }
        
        public void add(long itemId) {
            items.add(itemId);
            count = -1;
        }
        
        public static Item parse(String line) {
            String[] ss = line.split("\\t");
            long userId = Long.valueOf(ss[0]);
            int count = Integer.valueOf(ss[1]);
            Item item = new Item(userId);
            item.count = count;
            return item;
        }
        
        public static List<Item> parse(File file) throws IOException {
            List<String> lines = FileUtils.readLines(file);
            List<Item> items = new ArrayList<Item>(lines.size());
            for (String line:lines) {
                items.add(Item.parse(line));
            }
            return items;
        }
    }
}
