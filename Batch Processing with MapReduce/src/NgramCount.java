import java.io.IOException;
import java.io.StringReader;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NgramCount {

    public static class nGramMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }
        private static final String REGEX1 = "[^a-zA-Z']";
        private static final String HTTP = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"; // represnted by whitespace
        private static final String REF1 = "<ref.*?>";
        private static final String REF2 = "</ref>";
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	
        	String line = value.toString().toLowerCase();
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // XML parser refered to references[1]
            DocumentBuilder builder = null;
			try {
				builder = factory.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			}
            Document doc = null;
			try {
				doc = builder.parse(new InputSource(new StringReader(line)));
			} catch (SAXException e) {
				e.printStackTrace();
			}
            NodeList nList = doc.getElementsByTagName("revision");
            Element element = (Element)nList.item(0);
            String textContent = element.getElementsByTagName("text").item(0).getTextContent();
            textContent = StringEscapeUtils.unescapeXml(textContent); // handle unescape cases
            textContent = textContent.replaceAll(HTTP, " "); //replace all html by " "
            textContent = textContent.replaceAll(REF1, " "); //replace all <ref> by " "
            textContent = textContent.replaceAll(REF2, " "); //replace all <ref/> by " "
            textContent = textContent.replaceAll(REGEX1, " ");
            textContent = textContent.replaceAll("\\s*'\\B|\\B'\\s*", " "); //refered to references[0]
            textContent = textContent.replaceAll("\\s+", " ");
            textContent = textContent.trim();

            String[] textArray = textContent.split("\\s+");
            // output 1-gram
            for(int i = 0; i < textArray.length; i++){
                word.set(textArray[i].trim());
                context.write(word, one);

            }

            //output 2-gram
            String phase = "";
            for(int i = 0; i < textArray.length - 1; i++){
                phase = textArray[i].trim() + " " + textArray[i + 1].trim();
                word.set(phase);
                context.write(word, one);
            }

            //output 3-gram
            for(int i = 0; i < textArray.length - 2; i++){
                phase = textArray[i].trim() + " " + textArray[i+1].trim() + " " + textArray[i+2].trim();
                word.set(phase);
                context.write(word, one);
            }

            //output 4-gram
            for(int i = 0; i < textArray.length - 3; i++){
                phase = textArray[i].trim() + " " + textArray[i+1].trim() + " " + textArray[i+2].trim() + " " + textArray[i+3].trim();
                word.set(phase);
                context.write(word, one);
            }

            //output 5-gram
            for(int i = 0; i < textArray.length - 4; i++){
                phase = textArray[i].trim() + " " + textArray[i+1].trim() + " " + textArray[i+2].trim() + " " + textArray[i+3].trim() + " " + textArray[i+4].trim();
                word.set(phase);
                context.write(word, one);
            }
        }
    }

    public static class CombinerReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            if(sum > 2){
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NgramCount");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(nGramMapper.class);
        job.setCombinerClass(CombinerReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}