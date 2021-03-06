package com.example;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;

@Configuration
@EnableBatchProcessing
public class MongoTestJob {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	private boolean overwriteDefault = false;
	
	@Bean
	public Job job() throws Exception {
		return jobs.get("mongotest")
				.start(step1(reader(overwriteDefault), processor(), writer()))
				.build();
	}

	/**
	 * @todo Can the modifier be private and still work?
	 * @return
	 */
	@Bean
	public Step step1(ItemReader<DBObject> reader, 
			ItemProcessor<DBObject, DBObject> processor, 
			ItemWriter<DBObject> writer) {
		return steps.get("step1").<DBObject, DBObject> chunk(10)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}
	
	@Bean
	@StepScope
	public ItemReader<DBObject> reader(
			@Value("#{jobParameters['annotation.overwrite']}") final boolean overwrite) 
					throws Exception {
		MongoItemReader<DBObject> reader = new MongoItemReader<>();
		reader.setCollection("variants");
		
		Logger.getAnonymousLogger().warning("Overwrite value = " + overwrite);
		reader.setQuery(overwrite ? "{}" : "{ annot : { $exists : false } }");
		reader.setFields("{ chr : 1, start : 1, end : 1, ref : 1, alt : 1}");
		reader.setTargetType(DBObject.class);
		reader.setTemplate(mongoTemplate());

		Map<String, Direction> coordinatesSort = new HashMap<>();
		coordinatesSort.put("chr", Direction.ASC);
		coordinatesSort.put("start", Direction.ASC);
		reader.setSort(coordinatesSort);
		return reader;
	}

	@Bean
	@StepScope
	public ItemProcessor<DBObject, DBObject> processor() {
		return new VariantAnnotationProcessor();
	}

	@Bean
	@StepScope
	public ItemWriter<DBObject> writer() throws Exception {
		MongoItemWriter<DBObject> writer = new VariantAnnotationMongoItemWriter();
		writer.setCollection("variants");
		writer.setTemplate(mongoTemplate());
		return writer;
	}
	
	public MongoOperations mongoTemplate() throws Exception {		
		MongoTemplate mongoTemplate = new MongoTemplate(new MongoClient(), "test");
		return (MongoOperations) mongoTemplate;
		
	}
	
	class VariantAnnotationProcessor implements ItemProcessor<DBObject, DBObject> {

		int numUpdated;
		
		@Override
		public DBObject process(DBObject object) throws Exception {
			System.out.println(++numUpdated + " = " + object);
			object.put("annot", "SO:123");
			return object;
		}
		
	}
	
}
