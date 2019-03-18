package vn.com.techcombank.model;

public class Message {
	@Override
	public String toString() {
		return "Message [payload=" + payload + ", language=" + language + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((language == null) ? 0 : language.hashCode());
		result = prime * result + ((payload == null) ? 0 : payload.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (language == null) {
			if (other.language != null)
				return false;
		} else if (!language.equals(other.language))
			return false;
		if (payload == null) {
			if (other.payload != null)
				return false;
		} else if (!payload.equals(other.payload))
			return false;
		return true;
	}

	private String payload;
	private String language;

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

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
