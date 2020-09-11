package part1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;

public class TFWeighting extends Configured implements Tool {
	
	
	public static void main(String[] args) throws Exception {
		int rs = ToolRunner.run(new TFWeighting(), args);
		System.exit(rs);
	}
	//Mapreduce program
	//Target: To get tfweighting in a corpus of plain text documents

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "TermFreqWeighting");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable output = new IntWritable(1);
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String doc_content = lineText.toString();
			Text key = new Text();
			
			StringTokenizer st = new StringTokenizer(doc_content);
			
			
			String docName = context.getInputSplit().toString();
			docName = docName.substring(docName.lastIndexOf("/") + 1);
			String docID = docName.split("_")[1];
			while(st.hasMoreElements()){
				String word = normalizeWord(st.nextElement().toString());
				if (word == null)
					continue;
				Stemmer s = new Stemmer();
				s.add(word.toCharArray(), word.length());
				s.stem();
				key = new Text(s.toString()+ "\t"+docID);
				context.write(key, output);
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text,  DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			
			context.write(word, new DoubleWritable(FindWordFreq(sum)));
		
	}
}
    public static String normalizeWord(String word) {
        String normalizedWord = word.toLowerCase();
        normalizedWord = normalizedWord.replaceAll("[^a-z0-9]+","");
        if (normalizedWord != null && !normalizedWord.equals("")){
            
        	return normalizedWord;
        }
        else{
            return null;
        }

        
        
	}
	//Calculate the weigthing of term freq
    private static double FindWordFreq(int termCount)
	{
		double freq = 0.0;
		if(termCount> 0){
			freq = 1 + Math.log10(termCount);
		}
		return freq;
	}
    
	
}
