package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TfDocument;

public class CorpusTfReducer implements ReduceFunction<TfDocument>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3632516533323496414L;

	@Override
	public TfDocument call(TfDocument v1, TfDocument v2) throws Exception {
		// TODO Auto-generated method stub
		HashMap<String, Short> v3 = new HashMap<>(v1.getTermsFrequency());
		v2.getTermsFrequency().forEach(
				(key, value) -> v3.merge(key, value, (oldValue, newValue) -> {return (short) (oldValue + newValue);}));
		
		return new TfDocument(v3,null);
	}

}
