/**
 * 2013-2-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * @author xuhongfeng
 *
 */
public class CollectionUtils {
    public static long[] toArray(Iterator<Long> it) {
        List<Long> list = toList(it);
        return toArray(list);
    }
    
    public static List<Long> toList(Iterator<Long> it) {
        List<Long> list = new ArrayList<Long>();
        while (it.hasNext()) {
            list.add(it.next());
        }
        return list;
    }
    
    public static long[] toArray(Collection<Long> collection) {
        long[] array = new long[collection.size()];
        Iterator<Long> it = collection.iterator();
        int i=0;
        while (it.hasNext()) {
            array[i++] = it.next();
        }
        return array;
    }
    
    public static boolean isEmpty(int[] array) {
        return array==null || array.length==0;
    }

    public static boolean isEmpty(Collection<?> c) {
        if(c==null || c.size()==0) {
            return true;
        }
        return false;
    }

    public static <T> List<T> arrayToList(T[] array) {
       List<T> list = new ArrayList<T>();
       for (T item:array) {
           list.add(item);
       }
       return list;
    }

    public static String join(int[] array, String split) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int v:array) {
            String s = String.valueOf(v);
            if (!first) {
                sb.append(split);
            }
            first = false;
            sb.append(s);
        }
        return sb.toString();
    }

    public static String join(String[] array, String split) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s:array) {
            if (!first) {
                sb.append(split);
            }
            first = false;
            sb.append(s);
        }
        return sb.toString();
    }

    public static byte[] slice(byte[] array, int start, int len) {
        byte[] r = new byte[len];
        System.arraycopy(array, start, r, 0, len);
        return r;
    }

    public static int[] slice(int[] array, int start, int len) {
        int[] r = new int[len];
        System.arraycopy(array, start, r, 0, len);
        return r;
    }

    public static String[] slice(String[] array, int start, int len) {
        String[] r = new String[len];
        for (int i=0; i<len; i++) {
            r[i] = array[i+start];
        }
        return r;
    }

    public static int[] splitToInt(String s, String split) {
        if (StringUtils.isBlank(s)) {
            return new int[0];
        }
        String[] ss = s.split(split);
        return toIntArray(ss);
    }

    public static int[] toIntArray(String[] ss) {
        int[] r = new int[ss.length];
        for (int i=0; i < r.length; i++) {
            r[i] = Integer.valueOf(ss[i]);
        }
        return r;
    }

    public static int[] toIntArray(List<Integer> list) {
        return toPrimitive(list.toArray(new Integer[0]));
    }

    public static int[] toPrimitive(Integer[] array) {
        int[] r = new int[array.length];
        for (int i = 0; i < array.length; i++) {
            r[i] = array[i];
        }
        return r;
    }

    public static Integer[] toBoxed(int[] array) {
        Integer[] boxed = new Integer[array.length];
        for (int i=0; i<array.length; i++) {
            boxed[i] = Integer.valueOf(array[i]);
        }
        return boxed;
    }

    public static <T> boolean isEmpty(T[] array) {
        return array==null || array.length==0;
    }

    public static int[] remove(int[] array, int start, int len) {
        int[] r = new int[array.length - len];
        System.arraycopy(array, 0, r, 0, start);
        System.arraycopy(array, start+len, r, start, array.length-start-len);
        return r;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] remove(T[] array, int start, int len, Class<T> clazz) {
        T[] r = (T[]) Array.newInstance(clazz, array.length - len);
        System.arraycopy(array, 0, r, 0, start);
        System.arraycopy(array, start+len, r, start, array.length-start-len);
        return r;
    }

    public static <T> void filter(List<T> list, Tester<T> tester) {
        Iterator<T> it = list.iterator();
        while (it.hasNext()) {
            if (!tester.test(it.next())) {
                it.remove();
            }
        }
    }
    public static <T> boolean every(List<T> list, Tester<T> callback) {
        for (T data:list) {
            if (!callback.test(data)) {
                return false;
            }
        }
        return true;
    }

    public static interface Tester<T> {
        public boolean test(T data);
    }

    public static <T, R> R reduce(T[] array, Reducer<T, R> callback, R initialValue) {
        if (isEmpty(array)) {
            return initialValue;
        }
        R result = initialValue;
        for (int i=0; i<array.length; i++) {
            result = callback.reduce(result, array[i], i, array);
        }
        return result;
    }

    public static interface Reducer<T, R> {
        public R reduce(R previousValue, T currentValue, int index, T[] array);
    }

    public static <T, R> R[] map(T[] values, R[] emptyResults, Mapper<T, R> mapper) {
        for (int i=0; i<emptyResults.length; i++) {
            emptyResults[i] = mapper.map(values[i]);
        }
        return emptyResults;
    }

    public static interface Mapper<T, R> {
        public R map(T value);
    }

    private static final Reducer<Integer, String> INT_ARRAY_TO_STRING = new Reducer<Integer, String> () {
        @Override
        public String reduce(String previousValue, Integer currentValue,
                int index, Integer[] array) {
            if (StringUtils.isBlank(previousValue)) {
                if (index == array.length-1) {
                    return "[" + currentValue + "]";
                } else {
                    return "[" + currentValue;
                }
            } else {
                if (index == array.length-1) {
                    return previousValue + ", " + currentValue + "]";
                } else {
                    return previousValue + ", " + currentValue;
                }
            }
        }
    };
    public static String toString(int[] array) {
        return reduce(toBoxed(array), INT_ARRAY_TO_STRING, "");
    }
}
