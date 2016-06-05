package com.example;

import java.net.UnknownHostException;
import java.util.List;

import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class VariantAnnotationMongoItemWriter extends MongoItemWriter<DBObject> {

	private MongoTemplate template;

	
	public VariantAnnotationMongoItemWriter() throws UnknownHostException {
		template = new MongoTemplate(new MongoClient(), "test");
	}
	
	@Override
	protected void doWrite(List<? extends DBObject> items) {
		for (DBObject item : items) {
			template.getCollection("variants").update(
					new BasicDBObject("_id", item.get("_id")),
					new BasicDBObject("$set", new BasicDBObject().append("annot", item.get("annot"))));
		}
	}
	
}
