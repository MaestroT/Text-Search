package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


public class DocumentLengthMap implements MapFunction<NewsArticle,Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4803371802567864860L;

	@Override
	public Integer call(NewsArticle value) throws Exception {
		// TODO Auto-generated method stub
		List<ContentItem> paragraphs = value.getContents();
		int docLength = 0;
		for (ContentItem p:paragraphs) {
			String paragraph = p.getContent();
			String[] contents = paragraph.split(",");
			docLength += contents.length;
		}
		return docLength;
	}

}
