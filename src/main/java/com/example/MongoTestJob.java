package com.example;

import java.util.HashMap;
import java.util.Map;

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
	
	@Bean
	public Job job() throws Exception {
		return jobs.get("mongotest")
				.start(step1(reader(), processor(), writer()))
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
	public ItemReader<DBObject> reader() throws Exception {
		MongoItemReader<DBObject> reader = new MongoItemReader<>();
		reader.setCollection("variants");
		reader.setQuery("{ annot : { $exists : false } }");
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
		return new PrintItemProcessor();
	}

	@Bean
	@StepScope
	public ItemWriter<DBObject> writer() throws Exception {
		MongoItemWriter<DBObject> writer = new MongoItemWriter<>();
		writer.setCollection("variants");
		writer.setTemplate(mongoTemplate());
		return writer;
	}
	
	public MongoOperations mongoTemplate() throws Exception {		
		MongoTemplate mongoTemplate = new MongoTemplate(new MongoClient(), "test");
		return (MongoOperations) mongoTemplate;
		
	}
	
	class PrintItemProcessor implements ItemProcessor<DBObject, DBObject> {

		@Override
		public DBObject process(DBObject object) throws Exception {
			System.out.println(object);
			object.put("annot", "SO:123");
			return object;
		}
		
	}
	
}
