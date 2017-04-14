package com.amazonaws.lambda.demo;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LambdaFunctionHandler implements RequestHandler<String, String> {

    public String handleRequest(String input, Context context) {
        context.getLogger().log("Input: " + input);
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder
        						.standard()
        						.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://dynamodb.us-east-2.amazonaws.com", "us-east-2"))
        						.build();
        DynamoDB dynamoDB = new DynamoDB(client); 
        String tableName = "Movies";
        String message = "";
        try {
        	//CREATE TABLE -- START
        	 System.out.println("Attempting to create table; please wait...");
        	 Table table = dynamoDB.createTable(tableName,
        	 Arrays.asList(
        			 	new KeySchemaElement("year", KeyType.HASH), //Partition        	 key
    			 		new KeySchemaElement("title", KeyType.RANGE)), //Sort        	 key
        	 Arrays.asList(
			        	 new AttributeDefinition("year", ScalarAttributeType.N),
			        	 new AttributeDefinition("title",
		        			 	ScalarAttributeType.S)),
        	 			new ProvisionedThroughput(10L, 10L));
        	 table.waitForActive();
        	 System.out.println("Success. Table status: " +
        	 table.getDescription().getTableStatus());
        	//CREATE TABLE -- END
//------------------------------------------------------------------------------------------------------------------------------        	
        	//INSERT LIST INTO TABLE -- START
        	table = dynamoDB.getTable(tableName);
        	JsonParser parser = new JsonFactory().createParser(new File("moviedata.json"));
        	JsonNode rootNode = new ObjectMapper().readTree(parser);
        	Iterator<JsonNode> iter = rootNode.iterator();
        	ObjectNode currentNode;
        	while(iter.hasNext()) {
        		currentNode = (ObjectNode)iter.next();
        		int year = currentNode.path("year").asInt();
        		String title = currentNode.path("title").asText();
        		try {
					table.putItem(new Item().withPrimaryKey("year", year, "title", title)
											.withJSON("info", currentNode.path("info").toString()));
					System.out.println("PutItem succeeded: " + year + " " + title);

				} catch (Exception e) {
					System.err.println("Unable to add movie: " + year + " " + title);
					 System.err.println(e.getMessage());
					 break;
				}
        	}
        	//INSERT LIST INTO TABLE -- END
//------------------------------------------------------------------------------------------------------------------------------     
        	//CREATE A NEW ITEM - START
        	table = dynamoDB.getTable(tableName);
        	int year = 2015;
        	String title = "The Big New Movie";
        	final Map<String, Object> infoMap = new HashMap<String, Object>();
        	infoMap.put("plot", "Nothing happens at all.");
        	infoMap.put("rating", 0);
        	try {
				System.out.println("Adding new item...");
				PutItemOutcome outcome = table.putItem(new Item().withPrimaryKey("year", year, "title", title).withMap("info", infoMap));
				message = outcome.getPutItemResult().toString();
				System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
			} catch (Exception e) {

			}
        	//CREATE A NEW ITEM - END
//------------------------------------------------------------------------------------------------------------------------------          	
        	//READ ITEM - START
        	table = dynamoDB.getTable(tableName);
        	year = 2015;
        	title = "The Big New Movie";
        	GetItemSpec spec = new GetItemSpec().withPrimaryKey("year", year, "title", title);
        	Item outcome = table.getItem(spec);
        	message = outcome.toJSON();
        	//READ ITEM - END
//------------------------------------------------------------------------------------------------------------------------------  
        	//UPDATE ITEM - START
        	table = dynamoDB.getTable(tableName);
        	year = 2015;
        	title = "The Big New Movie";
        	UpdateItemSpec updateSpec = new UpdateItemSpec().withPrimaryKey("year",  year, "title", title)
    													.withUpdateExpression("set info.rating = :r, info.plot = :p, info.actors = :a")
    													.withValueMap(new ValueMap().withNumber(":r", 5.5)
    																				.withString(":p", "Everything happens all at once!")
    																				.withList(":a", Arrays.asList("Larry", "Moe", "Curly")))
    													.withReturnValues(ReturnValue.UPDATED_NEW);
        	UpdateItemOutcome updateOutcome = table.updateItem(updateSpec);
        	message =  updateOutcome.getItem().toJSONPretty();
        	//UPDATE ITEM - END
        } catch (Exception e) {
        	System.err.println("Unable to create table: ");
        	System.err.println(e.getMessage());
		}
        return "Table Creation :: " + message;
    }

}
