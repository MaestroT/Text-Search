package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TfDocument implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2667336266974179885L;
	private HashMap<String, Short> termsFrequency;
	private NewsArticle Document;
	private int DocLength;
	
	

	public TfDocument(HashMap<String, Short> termsFrequency, NewsArticle document) {
		super();
		this.termsFrequency = termsFrequency;
		this.Document = document;
	}
	
	public HashMap<String, Short> getTermsFrequency() {
		return termsFrequency;
	}
	
	public void setTermsFrequency(HashMap<String, Short> termsFrequency) {
		this.termsFrequency = termsFrequency;
	}
	
	public NewsArticle getDocument() {
		return Document;
	}
	
	public void setDocument(NewsArticle document) {
		this.Document = document;
	}
	
	public int getDocLength() {
		return DocLength;
	}
	
	public void setDocLength(int docLength) {
		this.DocLength = docLength;
	}

}
