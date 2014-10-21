package KMeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException{
        String line = value.toString();
        String[] fields = line.split(" ");
        List<ArrayList<Float>> centers = Assistance.getCenters(context.getConfiguration().get("centerpath"));
        int k = Integer.parseInt(context.getConfiguration().get("kpath"));
        float minDist = Float.MAX_VALUE;
        int centerIndex = k;
        //计算样本点到各个中心的距离，并把样本聚类到距离最近的中心点所属的类
        for (int i = 0; i < k; ++i){
            float currentDist = 0;
            for (int j = 0; j < fields.length; ++j){
                float tmp = Math.abs(centers.get(i).get(j + 1) - Float.parseFloat(fields[j]));
                currentDist += Math.pow(tmp, 2);
            }
            if (minDist > currentDist){
                minDist = currentDist;
                centerIndex = i;
            }
        }
        context.write(new IntWritable(centerIndex), new Text(value));
    }
}