package pagerank;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.collect.Iterables;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
class ExtractedInfo implements Serializable{
	Integer pageId;
	String pageTitle;
	List<String> outboundLinks;
	ExtractedInfo(Integer pageid, String pagetitle, List<String> outboundlinks){
		this.pageId = pageid;
		this.pageTitle = pagetitle;
		this.outboundLinks = outboundlinks;
	}
}

public class WikiPageRank {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
        File f = new File(args[2]);
        FileUtils.cleanDirectory(f); //clean out directory
        FileUtils.forceDelete(f); //delete directory
		
		
		SparkConf conf = new SparkConf().setAppName("PageRank").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long startTime = System.nanoTime();
		JavaRDD<String> lines = sc.textFile(args[1]);
		
		JavaRDD<ExtractedInfo> extractedinfo  = lines.map(s->{
			String[] splits = s.split("\t");
			List<String> outboundlist = new ArrayList<>();
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			InputSource src = new InputSource();
			src.setCharacterStream(new StringReader(splits[3]));

			Document doc = builder.parse(src);
			NodeList list = doc.getElementsByTagName("target");
			
			for(int i=0; i <list.getLength();i+=1){
				outboundlist.add(list.item(i).getTextContent());
			}
			
			return new ExtractedInfo(Integer.parseInt(splits[0]),splits[1],outboundlist);
		}).cache();
		
		JavaRDD<String> validpages = extractedinfo.map(s->{
			return s.pageTitle;
		});
		
		// all the worker nodes have the validpage title list
		Broadcast<Set<String>> broadcastinfo = sc.broadcast(new HashSet<>(validpages.collect()));
		

		JavaPairRDD<String, String> links = extractedinfo.flatMapToPair(
				new PairFlatMapFunction<ExtractedInfo, String, String>() {

					@Override
					public Iterator<Tuple2<String, String>> call(ExtractedInfo e) throws Exception {
						// TODO Auto-generated method stub
						List<Tuple2<String, String>> list = new ArrayList<>();
						for(String link : e.outboundLinks){
							list.add(new Tuple2<String, String>(e.pageTitle, link));
						}
						
						return list.iterator();
					}
					
				});
		
		
		JavaPairRDD<String, Iterable<String>> validlinks = links.filter(
				new Function<Tuple2<String,String>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, String> link) throws Exception {
				// TODO Auto-generated method stub
				return broadcastinfo.value().contains(link._2()) && !link._1().equals(link._2());
			}
		}).distinct().groupByKey().cache();
	//	validlinks.saveAsTextFile(args[2]);
		
	    JavaPairRDD<String, Double> ranks = validlinks.mapValues(new Function<Iterable<String>, Double>() {
	      @Override
	      public Double call(Iterable<String> rs) {
	        return 1.0;
	      }
	    });
	   
	    //  PageRank algorithm.
	    for (int i = 0; i < Integer.parseInt(args[3]); i++) {
	      
	      JavaPairRDD<String, Double> contribs = validlinks.join(ranks).values()
	        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
	          @Override
	          public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
		    int totallinkcount = Iterables.size(s._1);
	            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
	            for (String outlink : s._1) {
	              results.add(new Tuple2<String, Double>(outlink, s._2() / totallinkcount));
	            }
	            return results.iterator();
	          }
	      });

	      // Re-calculates URL ranks based on neighbor contributions.
	      ranks = contribs.reduceByKey((a,b)->a+b).mapValues(new Function<Double, Double>() {
	        @Override
	        public Double call(Double sum) {
	          return 0.15 + sum * 0.85;
	        }
	      }).cache();
	    }
	    List<Tuple2<String, Double>> ranklist = ranks.collect();
	    ranklist.sort(new Comparator<Tuple2<String, Double>>() {

			@Override
			public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
				// TODO Auto-generated method stub
				return o2._2() > o1._2() ? 1 : -1;
			}
		});
	    // Collects all URL ranks and dump them to console.
	    BufferedWriter filewrite = new BufferedWriter(new FileWriter(args[2]));
	    for(int i=1; i<=100;i+=1){
	    	filewrite.append(new String(Integer.toString(i)+". "+ranklist.get(i)._1()));
	    	filewrite.append("\n");
	    }
	    long endTime = System.nanoTime();
	    long duration = (endTime - startTime)/1000000;
	    filewrite.append("Time taken to compute using Spark Implementation is "+Double.toString(duration));
	    filewrite.close();
	    
	    
	    //University Rankings
	    JavaPairRDD<String, Double> univranks = ranks.filter(new Function<Tuple2<String,Double>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, Double> page) throws Exception {
				// TODO Auto-generated method stub
				return page._1().toLowerCase().contains("university") || 
						page._1().toLowerCase().contains("institution") ||
						page._1().toLowerCase().contains("institute");
			}
		});
	    
	    List<Tuple2<String, Double>> univranklist = univranks.collect();
	    univranklist.sort(new Comparator<Tuple2<String, Double>>() {

			@Override
			public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
				// TODO Auto-generated method stub
				return o2._2() > o1._2() ? 1 : -1;
			}
		});
	    // Collects all URL ranks and dump them to console.
	    BufferedWriter filewrite2 = new BufferedWriter(new FileWriter(args[4]));
	    for(int i=1; i<=100;i+=1){
	    	filewrite2.append(new String(Integer.toString(i)+". "+univranklist.get(i)._1()));
	    	filewrite2.append("\n");
	    }
	    filewrite2.close();
	    
	}

}
