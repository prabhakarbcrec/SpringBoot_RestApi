package com.condidates.getDetails;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.condidates.getDetails.connectToDatabase.GetPostgresConnection;
import com.condidates.getDetails.cost.AppConstants;
import com.condidates.getDetails.model.Base;
import com.condidates.getDetails.service.KafKaConsumerService;
import com.condidates.getDetails.service.SendMesssageService;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.nexmo.client.NexmoClient;
import com.nexmo.client.NexmoClientException;
import com.nexmo.client.auth.AuthMethod;
import com.nexmo.client.auth.TokenAuthMethod;
import com.nexmo.client.sms.SmsSubmissionResult;
import com.nexmo.client.sms.messages.TextMessage;

@RestController
@RequestMapping("/start")
public class ControllerToGetAllMesssage {

	@RequestMapping(value = "/messsage", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ArrayList<Base> getMessage() {
		ArrayList<Base> messager = new ArrayList<Base>();
		Base data1 = new Base("i am coming", "7001536970", "7098080705");
		Base data2 = new Base("i am going", "7001536970", "7098080706");
		Base data3 = new Base("i am not", "7001536970", "7098080707");
		Base data4 = new Base("i am great", "7001536970", "7098080705");
		Base data5 = new Base("i am no foolish", "7001536970", "7098080705");
		Base data6 = new Base("i am police", "7001536970", "7098080705");
		Base data7 = new Base("i am egg", "7001536970", "7098080705");
		Base data8 = new Base("i am nothing", "7001536970", "7098080705");
		Base data9 = new Base("i am prabhakar", "7001536970", "7098080705");
		Base data10 = new Base("i am munna", "7001536970", "7098080705");
		Base data11 = new Base("i am jai hind", "7001536970", "7098080705");
		messager.add(data1);
		messager.add(data2);
		messager.add(data3);
		messager.add(data4);
		messager.add(data5);
		messager.add(data6);
		messager.add(data7);
		messager.add(data8);
		messager.add(data9);
		messager.add(data10);
		messager.add(data11);

		return messager;
	}

	private final SendMesssageService producerService;

	@Autowired
	public ControllerToGetAllMesssage(SendMesssageService producerService) {
		this.producerService = producerService;
	}

	@Autowired
	KafKaConsumerService consumerRef;

	@PostMapping(value = "/publish")
	public void sendMessageToKafkaTopic(@RequestParam("topic") String topicName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, JSONException, SQLException {
		final String uri = "http://localhost:8080/start/messsage";

		RestTemplate restTemplate = new RestTemplate();
		String result = restTemplate.getForObject(uri, String.class);
		org.json.JSONArray jsonArr = new org.json.JSONArray(result);

		for (int i = 0; i < jsonArr.length(); i++) {
			org.json.JSONObject jsonObj = jsonArr.getJSONObject(i);

			this.producerService.sendMessage(
					jsonObj.getString("from") + "," + jsonObj.getString("to") + "," + jsonObj.getString("body"),
					topicName, String.valueOf(i));
			KeepSameDataToDataBase(jsonObj.getString("from") + "," + jsonObj.getString("to") + "," + jsonObj.getString("body"),String.valueOf(i));
		}

	}

	private void KeepSameDataToDataBase(String message, String id) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection connectionOfPostgres=GetPostgresConnection.getConnectionInstance();
		String insertQuery = "insert into kafkaData_is_a_Table_name values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement insertStmt = connectionOfPostgres.prepareStatement(insertQuery);
		insertStmt.setString(1, String.valueOf(id));
		insertStmt.setString(2, String.valueOf(message));
		insertStmt.execute();
		System.out.println("inserted data into database");
	}

	@PostMapping(value = "/publishStatus")
	public void sendMessageToKafkaTopic2(@RequestParam("topic") String topicName,
			@RequestParam("message") String message, @RequestParam("key") String key) {

		this.producerService.sendMessage(message, topicName, key);
	

	}

	@RequestMapping(value = "/producer/consume-message/{topic}", method = { RequestMethod.GET })
	@ResponseBody
	public void consumeMessage(@PathVariable String topic) {

		ConsumerFactory<String, Object> consumerFactory = getConsumerFactoryInstance();

		Consumer<String, Object> consumer = consumerFactory.createConsumer();

		consumer.subscribe(Collections.singletonList("sampleTopic"));

		// poll messages from last 10 days
		ConsumerRecords<String, Object> consumerRecords = consumer.poll(Duration.ofDays(10));

		// print on console or send back as a string/json. Feel free to change
		// controller function implementation for ResponseBody
		consumerRecords.forEach(action -> {
			String toFromBody[] = action.value().toString().split(",");
			String key = action.key();
			try {
				SendMessageToOutOfWorld(toFromBody[0], toFromBody[1], toFromBody[2], key);
			} catch (IOException | NexmoClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		});

	}

	private void SendMessageToOutOfWorld(String to, String from, String body, String key)
			throws IOException, NexmoClientException {
		AuthMethod auth = new TokenAuthMethod("abcd", "degfhrtXmxhdhgfjghghdf");//please keep here original api key 
		NexmoClient client = new NexmoClient(auth);
		TextMessage message = new TextMessage(from, to, "Hello from Nexmo!");

		SmsSubmissionResult[] responses = client.getSmsClient().submitMessage(message);
		for (SmsSubmissionResult response : responses) {
			sendBackToKafka(response.getStatus(), key, AppConstants.TOPIC_NAME);
		}

	}

	private void sendBackToKafka(int status, String key, String topic) {
		final String uri = "http://localhost:8080/start/publishStatus?topic=" + topic + "&message=" + status + "&key="
				+ key;

		RestTemplate restTemplate = new RestTemplate();
		String result = restTemplate.getForObject(uri, String.class);

	}

	public ConsumerFactory<String, Object> getConsumerFactoryInstance() {
		Map<String, Object> configs = new java.util.HashMap<>();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, "anyIdForGroup");
		configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
		configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
		ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
		return consumerFactory;
	}

}
