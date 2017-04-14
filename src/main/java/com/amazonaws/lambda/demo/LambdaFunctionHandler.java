package com.amazonaws.lambda.demo;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<String, String> {

    public String handleRequest(String input, Context context) {
        context.getLogger().log("Input: " + input);
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(new EnvironmentVariableCredentialsProvider());
        DynamoDBMapper mapper = new DynamoDBMapper(client); 
//        Employee employee = new Employee();
//        employee.setEmpId(101);
//        employee.setAddress("711 High St");
//        employee.setFirstName("Suresh");
//        employee.setLastName("Downtown");
//        mapper.save(employee);
        Employee e1 = new Employee();
        e1.setEmpId(100);
    	Employee e = mapper.load(e1);
        return "Hello, "+e.getFirstName() + " " + e.getLastName();
    }

}
