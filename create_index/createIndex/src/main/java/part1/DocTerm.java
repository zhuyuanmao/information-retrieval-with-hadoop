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
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;

public class DocTerm extends Configured implements Tool {
	
	
	public static void main(String[] args) throws Exception {
		int rs = ToolRunner.run(new DocTerm(), args);
		System.exit(rs);
	}
	//Mapreduce program
	//Target: To get inverted index in a corpus of plain text documents


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "DocTerm");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static class Map extends Mapper<Object, Text, Text, Text> {
		//private final static IntWritable output = new IntWritable(1);
		private Text term = new Text();
		private Text doc = new Text();
		public void map(Object key, Text lineText, Context context) throws IOException, InterruptedException {
			String doc_content = lineText.toString();
			StringTokenizer st = new StringTokenizer(doc_content);
			
			
			
			String docName = context.getInputSplit().toString();					//To get docName.
			docName = docName.substring(docName.lastIndexOf("/") + 1);				
			String docID = docName.split("_")[1];									//Split context by whitespace

			while(st.hasMoreElements()){
				String word = normalizeWord(st.nextElement().toString());			//Take way all special character
				if (word == null)													//Skip special character
					continue;
				Stemmer s = new Stemmer();											//Porter Stemmer. Normalizing term
				s.add(word.toCharArray(), word.length());
				s.stem();
				term.set(s.toString());
				doc.set(docID);
				context.write(term, doc);
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			sb.append("< ");
			boolean first = true;
			for (Text value : values) {										//Add docId into the value.

				if (first) {
					sb.append(value.toString());
					first = false;
				}
				if (value == null)
					continue;
				if (sb.lastIndexOf(value.toString()) < 0) {
					sb.append(", ");
					sb.append(value.toString());
					}
				}
			sb.append(" >");
			
			result.set(sb.toString());			
			context.write(word, result);
		
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
