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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;

public class DocFreqCounter extends Configured implements Tool {
	
	
	public static void main(String[] args) throws Exception {
		int rs = ToolRunner.run(new DocFreqCounter(), args);
		System.exit(rs);
	}
	//Mapreduce program
	//Target: To get term frequency in a corpus of plain text documents


	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "DocFreqCounter");
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
			Text term = new Text();
			
			StringTokenizer st = new StringTokenizer(doc_content);							//Split context by whitespace
			String docName = context.getInputSplit().toString();							//To get docName.
			docName = docName.substring(docName.lastIndexOf("/") + 1);						//To get docID
			while(st.hasMoreElements()){
				String word = normalizeWord(st.nextElement().toString());					//Take way all special character.
				if (word == null)															//Skip special character
					continue;
				Stemmer s = new Stemmer();													//Porter Stemmer. Normalizing term
				s.add(word.toCharArray(), word.length());
				s.stem();
				term = new Text(s.toString());
				context.write(term, output);
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {												//Reduce the sum.
				sum += count.get();	
			}
			
			context.write(word, new IntWritable(sum));
		
	}
}

	//function: normalizedWord: covert given string to lower case and remove all special characters
	//Parameter: String word
	//Return: normalized word or null.
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
    
	
}