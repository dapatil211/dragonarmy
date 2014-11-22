import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import libsvm.*;

public class SVMLearn{

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(SVMLearn.class);
		conf.setJobName("wordcount");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	    JobClient.runJob(conf);

	}
	
	 public static class Map extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {

		@Override
		public void map(IntWritable arg0, Text arg1,
				OutputCollector<IntWritable, Text> results, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			PrintWriter out = new PrintWriter("input" + arg0.get());
			out.println(arg1.toString());
			out.close();
			String[] args = {("input" + arg0.get())};
			svm_train.main(args);
			svm_model model = svm.svm_load_model(args[0] + ".model");
			int[] labels = model.label;
			svm_node[][] SV = model.SV;
			StringBuilder s = new StringBuilder();
			for(int i = 0; i < labels.length; i++){
				s.append(labels[i]);
				for(int j = 0; j < SV[i].length; j++){
					s.append(" " + SV[i][j].index + ":" + SV[i][j].value);
				}
				s.append("\n");
			}
			results.collect(new IntWritable((arg0.get()+1) / 2), new Text(s.toString()));
		}
		 
	 }
	 
	 public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable arg0, Iterator<Text> arg1,
				OutputCollector<IntWritable, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			StringBuilder s = new StringBuilder();
			while(arg1.hasNext()){
				s.append(arg1.next().toString());
			}
			arg2.collect(arg0, new Text(s.toString()));
		}
	 }
	 

}
