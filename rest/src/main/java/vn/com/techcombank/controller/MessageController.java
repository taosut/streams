package vn.com.techcombank.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import vn.com.techcombank.model.Message;
import vn.com.techcombank.producer.Sender;

@RestController
public class MessageController {
	
	@Autowired
	private Sender sender;

	@RequestMapping(method = RequestMethod.POST, value = "/messaging/send", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<HttpStatus> postMessage(@RequestBody Message msg) {
		if (msg.getPayload() != null) {
			sender.send(msg.getPayload());
			return ResponseEntity.ok(HttpStatus.OK);
		}
		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
	}

}
