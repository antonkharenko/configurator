package com.ogp.configurator.examples;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class FixedCurrencyRates {
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((rates == null) ? 0 : rates.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FixedCurrencyRates other = (FixedCurrencyRates) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (rates == null) {
			if (other.rates != null)
				return false;
		} else if (!rates.equals(other.rates))
			return false;
		return true;
	}

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