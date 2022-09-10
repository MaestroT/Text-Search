package uk.ac.gla.dcs.bigdata.providedfunctions;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;


/**
 * Converts a Row containing a String Json news article into a NewsArticle object 
 * @author Richard
 *
 */
public class NewsFormaterMap implements MapFunction<Row,NewsArticle> {

	private static final long serialVersionUID = -4631167868446468097L;

	private transient ObjectMapper jsonMapper;
	private transient TextPreProcessor processor;
	
	@Override
	public NewsArticle call(Row value) throws Exception {

		if (jsonMapper==null) jsonMapper = new ObjectMapper();
		if (processor==null) processor = new TextPreProcessor();
		
		NewsArticle article = jsonMapper.readValue(value.mkString(), NewsArticle.class);

		// Title PreProcess
		List<String> newTitleTermsList = processor.process(article.getTitle());
		String newTitle = String.join(",", newTitleTermsList);
		String id = article.getId();
		String article_url = article.getArticle_url();
		String author = article.getAuthor();
		long published_date = article.getPublished_date();
		String type = article.getType();
		String source = article.getSource();
		
//		System.out.println(newTitle); //test
		// Paragraph Content PreProcess
		List<ContentItem> contentItem = article.getContents(); // get original contentItem
		List<ContentItem> newcontentItem = new ArrayList<ContentItem>(); // Instantiate a new ContentItem List
		int count = 0; // record number of paragraph
		for (ContentItem cI:contentItem) {
			if(cI.getSubtype() != null && count < 5) {
				if( cI.getSubtype().equals("paragraph") ) {
					count++;
					List<String> newContentTermsList = processor.process(cI.getContent());
//					System.out.println(count);  // test
					String newContentTerms = String.join(",", newContentTermsList);
					ContentItem newCI = new ContentItem();    // Instantiate a new ContentItem
					newCI.setContent(newContentTerms);
					newcontentItem.add(newCI);
//					System.out.println(newContentTerms); // test
				}
			}
		}

		// Instantiate a new NewsArticle
		NewsArticle newNA = new NewsArticle(id,
				article_url,
				newTitle,
				author,
				published_date,
				newcontentItem,
				type,
				source);
		return newNA;
	}
		
		
	
}

