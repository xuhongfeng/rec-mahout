package hongfeng.xu.rec.mahout.chart;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Result {
    private final Map<Integer, Double> map;
    
    public Result() {
        map = new HashMap<Integer, Double>();
    }
    
    public void put(int N, double rate) {
        map.put(N, rate);
    }
    
    public void clear() {
        map.clear();
    }
    
    public List<Integer> listN() {
        Set<Integer> setN = map.keySet();
        List<Integer> listN = new ArrayList<Integer>();
        listN.addAll(setN);
        Collections.sort(listN);
        return listN;
    }
    
    public double getValue(int N) {
        return map.get(N);
    }
}