package com.condidates.getDetails.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate; 
import org.springframework.stereotype.Service;


@Service
public class SendMesssageService {
	private static final Logger logger = 
            LoggerFactory.getLogger(SendMesssageService.class);
     
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
 
    public void sendMessage(String message, String topicName,String key) 
    {
    	
        logger.info(String.format("Message sent -> %s", message));
        this.kafkaTemplate.send(topicName,key, message);
    }

}
