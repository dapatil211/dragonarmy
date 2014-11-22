import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
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
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 200000);
		conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
	    conf.setInputFormat(InpFormat.class);
	    FileInputFormat.setInputPaths(conf, args[0]);
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
	public class InpFormat extends FileInputFormat<IntWritable, Text> {
		public InpFormat() {
			// TODO Auto-generated constructor stub
		}
		@Override
		public RecordReader<IntWritable, Text> getRecordReader(InputSplit input,
				JobConf job, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			reporter.setStatus(input.toString());
		    return new InpRecordReader(job, (FileSplit)input);
		}
		
	}
	static int block = 0;

	public class InpRecordReader implements RecordReader<IntWritable, Text>{
		private final int NLINESTOPROCESS = 200000;
		private LineReader in;
		private IntWritable key;
		private Text value = new Text();
		private long start =0;
		private long end =0;
		private long pos =0;
		private int maxLineLength;
		public InpRecordReader(JobConf job, FileSplit split)throws IOException{
	        final Path file = split.getPath();
	        
	        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
	        FileSystem fs = file.getFileSystem(job);
	        start = split.getStart();
	        end= start + split.getLength();
	        boolean skipFirstLine = false;
	        FSDataInputStream filein = fs.open(split.getPath());
	 
	        if (start != 0){
	            skipFirstLine = true;
	            --start;
	            filein.seek(start);
	        }
	        in = new LineReader(filein,job);
	        if(skipFirstLine){
	            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
	        }
	        this.pos = start;
		}
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			if (in != null) {
	            in.close();
	        }
		}

		@Override
		public IntWritable createKey() {
			// TODO Auto-generated method stub
			block++;
			return new IntWritable(block);
		}

		@Override
		public Text createValue(){
			// TODO Auto-generated method stub
			if (key == null) {
	            key = new IntWritable();
	        }
	        key.set(block);
	        if (value == null) {
	            value = new Text();
	        }
	        value.clear();
	        final Text endline = new Text("\n");
	        int newSize = 0;
	        for(int i=0;i<NLINESTOPROCESS;i++){
	            Text v = new Text();
	            while (pos < end) {
	                try {
						newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                value.append(v.getBytes(),0, v.getLength());
	                value.append(endline.getBytes(),0, endline.getLength());
	                if (newSize == 0) {
	                    break;
	                }
	                pos += newSize;
	                if (newSize < maxLineLength) {
	                    break;
	                }
	            }
	        }
	        if (newSize == 0) {
	            key = null;
	            value = null;
	        }
	        return value;
		}

		@Override
		public long getPos() throws IOException {
			// TODO Auto-generated method stub
			return pos;
		}

		@Override
		public float getProgress() throws IOException {
			// TODO Auto-generated method stub
			return (float) pos / end;
		}

		@Override
		public boolean next(IntWritable arg0, Text arg1) throws IOException {
			// TODO Auto-generated method stub
			arg0=createKey();
			arg1=createValue();
			return true;
		}
		
	}
	
	

}
