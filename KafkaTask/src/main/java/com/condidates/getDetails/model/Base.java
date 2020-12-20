package com.condidates.getDetails.model;

public class Base {
	String to;
	String from;
	String body;
	public Base(String body,String to,String from) {
		this.body=body;
		this.from=from;
		this.to=to;
		
	}
	public String getTo() {
		return to;
	}
	public void setTo(String to) {
		this.to = to;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}

}
