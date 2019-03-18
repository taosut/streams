package vn.com.techcombank.model;

public class Message {
	private String payload;

	public Message() {
	
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public String getPayload() {
		return payload;
	}

	public Message(String msg) {
		this.payload = msg;
	}
}
