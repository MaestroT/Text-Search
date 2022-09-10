package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.TfDocument;

public class QueryResultFlatMap implements FlatMapFunction<TfDocument, RankedResult>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5627253310673897864L;
	Set<String> queryTerms;
	Broadcast<TfDocument> bcallFrequencyinCorpus;
	Integer TotalDocumentLength;
	double averageDocumentLengthInCorpus;
	long totalDocsInCorpus;

	public QueryResultFlatMap(Set<String> queryTerms, Broadcast<TfDocument> bcallFrequencyinCorpus,
			Integer totalDocumentLength, double averageDocumentLengthInCorpus, long totalDocsInCorpus) {
		super();
		this.queryTerms = queryTerms;
		this.bcallFrequencyinCorpus = bcallFrequencyinCorpus;
		TotalDocumentLength = totalDocumentLength;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
	}

	@Override
	public Iterator<RankedResult> call(TfDocument t) throws Exception {
		// TODO Auto-generated method stub
		short termFrequencyInCurrentDocument = 0;
		int totalTermFrequencyInCorpus = 0;
		double score = 0.0;
		
		for (String qT:queryTerms) {
			if (t.getTermsFrequency().containsKey(qT)) {
				termFrequencyInCurrentDocument += t.getTermsFrequency().get(qT);
			}
			if (bcallFrequencyinCorpus.getValue().getTermsFrequency().containsKey(qT)) {
				totalTermFrequencyInCorpus += bcallFrequencyinCorpus.getValue().getTermsFrequency().get(qT);
			}
			
			
			double DPHscore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, t.getDocLength(), averageDocumentLengthInCorpus, totalDocsInCorpus);
//			System.out.println(DPHscore);
			if (Double.isNaN(DPHscore)) {
				DPHscore = 0.0;
			}
			score += DPHscore;
		}
		// The DPH score for a <document,query> pair is the average of the DPH scores for each <document,term> pair (for each term in the query).
		score = score / queryTerms.size();
//		System.out.println(score);
		RankedResult rankResult = new RankedResult(t.getDocument().getId(), t.getDocument(), score);
		if (score < Double.MIN_VALUE) {
			List<RankedResult> ResultList = new ArrayList<RankedResult>(0);
			return ResultList.iterator();
		}
		else {
			List<RankedResult> ResultList = new ArrayList<RankedResult>(1);
		    ResultList.add(rankResult);
		    return ResultList.iterator();
		}
		
		
	}

}
