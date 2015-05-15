package com.ogp.configurator.examples;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class FixedCurrencyRates {
	private Map<String, BigDecimal> rates;
	private String key;

	public FixedCurrencyRates() {
		rates = new HashMap<String, BigDecimal>();
	}
	
	public FixedCurrencyRates(String key) {
		rates = new HashMap<String, BigDecimal>();
		this.setKey(key);
	}
	
	/**
	 * @return the rates
	 */
	public Map<String, BigDecimal> getRates() {
		return rates;
	}


	/**
	 * @param rates the rates to set
	 */
	public void setRates(Map<String, BigDecimal> rates) {
		this.rates = rates;
	}


	
	
	public FixedCurrencyRates addRate(String currency, BigDecimal rate) {
		rates.put(currency, rate);
		return this;
	}
	
	public BigDecimal getRateForCurrency(String currency) {
		return rates.get(currency);
	}

	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}
}