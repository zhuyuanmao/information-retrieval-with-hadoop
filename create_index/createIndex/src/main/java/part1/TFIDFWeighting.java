package part1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class TFIDFWeighting extends Configured implements Tool {
	private static final TFWeighting tfc = new TFWeighting();
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(tfc, args);
		int rs = ToolRunner.run(new TFIDFWeighting(), args);
		System.exit(rs);
	}
	//Mapreduce program
	//Target: To get tf-idf weighting of correspoding iterm with dociD in a corpus of plain text documents


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		FileSystem fnumber =  FileSystem.get(tfc.getConf()); //To get the number of files in given directory. 
		
		FileStatus[] status = fnumber.listStatus(new Path(args[0]));
		
		Configuration conf = new Configuration();
		
		conf.set("weighting", Integer.toString(status.length));
		
		Job job = Job.getInstance(conf, "TFIDFWeightingBuilder");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			
			String doc_content = lineText.toString();
			String[] l = doc_content.split("\t");
			//doc_content = doc_content.replace("\t", "="); 							//Set a delimiter
			
			String key = l[0]+"\t"+l[1];
			String value = "="+l[2];
			doc_content = key+value;
			context.write(new Text(key), new Text(value));							//Write a buffer 
			
			
		
			
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text,  DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			Double size = Double.parseDouble(conf.get("weighting"));
			
			
			double c = 0.0;
			ArrayList<String> tfs = new ArrayList<String>();
			
			for(Text temp : counts){
				String v = temp.toString();
				c++;
				tfs.add(v);
			}

			for(int i=0;i<tfs.size();i++){
				String docName = tfs.get(i);
				String docId = docName.substring(0, docName.lastIndexOf("="));
				Double tf = Double.parseDouble(docName.substring(docName.indexOf("=")+1));
				context.write(new Text(word + docId), new DoubleWritable(CalculateFreq(size,c)*tf));
			}

			
		
		
	}
}
	// Calculate Inverse Document Frequency
    private static double CalculateFreq(double totalDocs, double termFreq){
 
 			return Math.log10(1+(totalDocs / termFreq));
 		}

	

}
