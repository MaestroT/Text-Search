package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TfDocument;

public class TfFilterFlatMap implements FlatMapFunction<NewsArticle, TfDocument>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6850582958379293980L;
	
	// Global Data
	Set<String> broadcastqueryTerms;     // we name it as "broadcast" but it is no longer broadcast

	public TfFilterFlatMap(Set<String> queryTerms) {
		super();
		this.broadcastqueryTerms = queryTerms;
	}

	@Override
	public Iterator<TfDocument> call(NewsArticle t) throws Exception {
		// TODO Auto-generated method stub
		List<ContentItem> docContents = t.getContents();
		TfDocument tfDoc = new TfDocument(null,t);
		HashMap<String, Short> termFrequencyinDoc = new HashMap<String, Short>(); 
		int docLength = 0;
		
		String title = t.getTitle();
		String [] titleWords = title.split(" ");
		for (String tW:titleWords) {
			if (broadcastqueryTerms.contains(tW)) {
				if (!termFrequencyinDoc.containsKey(tW)) {
					termFrequencyinDoc.put(tW, (short) 1);
				}
				else {
					termFrequencyinDoc.put(tW, (short) (termFrequencyinDoc.get(tW)+1));
				}
			}
		}
		
		
		for (ContentItem dC: docContents) {
			String paragraph = dC.getContent();
			String[] paragraphArray = paragraph.split(",");
			docLength += paragraphArray.length;
			
			for (String p:paragraphArray) {
				if (broadcastqueryTerms.contains(p)) {
					if (!termFrequencyinDoc.containsKey(p)) {
						termFrequencyinDoc.put(p, (short) 1);
					}
					else {
						termFrequencyinDoc.put(p, (short) (termFrequencyinDoc.get(p)+1));
					}
				}
			}
		}
		
//		System.out.println(docLength);
//		System.out.println(termFrequencyinDoc);
		// set to structure
		tfDoc.setDocLength(docLength);
		tfDoc.setTermsFrequency(termFrequencyinDoc);
		
		if (termFrequencyinDoc.isEmpty()) {
			List<TfDocument> tfDocList = new ArrayList<TfDocument>(0);
			return tfDocList.iterator();
		}
		else {
			List<TfDocument> tfDocList = new ArrayList<TfDocument>(1);
			tfDocList.add(tfDoc);
//			System.out.println(tfDoc.getDocLength());
//			System.out.println(tfDoc.getTermsFrequency());
			return tfDocList.iterator();
		}
		
	}

}
