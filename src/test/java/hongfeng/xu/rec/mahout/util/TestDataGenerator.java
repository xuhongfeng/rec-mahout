/**
 * 2013-3-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import java.util.Random;

/**
 * @author xuhongfeng
 *
 */
public class TestDataGenerator {
    public static int[] genRandomIntArray(int size) {
        Random random = new Random();
        int[] array = new int[size];
        for (int i=0; i<size; i++) {
            array[i] = i;
        }
        for (int i=0; i<size; i++) {
            int k = random.nextInt(size);
            int t = array[i];
            array[i] = array[k];
            array[k] = t;
        }
        return array;
    }
    
    public static float[] genRandomFloatArray(int size) {
        float[] array = new float[size];
        int[] intArray = genRandomIntArray(size);
        for (int i=0; i<size; i++) {
            array[i] = intArray[i];
        }
        return array;
    }
    
    public static double[] genRandomDoubleArray(int size) {
        double[] array = new double[size];
        int[] intArray = genRandomIntArray(size);
        for (int i=0; i<size; i++) {
            array[i] = intArray[i];
        }
        return array;
    }
}
