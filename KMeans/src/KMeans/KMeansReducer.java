package KMeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> value, Context context)
    throws IOException, InterruptedException{
        List<ArrayList<Float>> assistList = new ArrayList<ArrayList<Float>>();
        String tmpResult = "";
        for (Text val : value){
            String line = val.toString();
            String[] fields = line.split(" ");
            List<Float> tmpList = new ArrayList<Float>();
            for (int i = 0; i < fields.length; ++i){
                tmpList.add(Float.parseFloat(fields[i]));
            }
            assistList.add((ArrayList<Float>) tmpList);
        }
        //计算新的聚类中心
        for (int i = 0; i < assistList.get(0).size(); ++i){
            float sum = 0;
            for (int j = 0; j < assistList.size(); ++j){
                sum += assistList.get(j).get(i);
            }
            float tmp = sum / assistList.size();
            if (i == 0){
                tmpResult += tmp;
            }
            else{
                tmpResult += " " + tmp;
            }
        }
        Text result = new Text(tmpResult);
        context.write(key, result);
    }
}