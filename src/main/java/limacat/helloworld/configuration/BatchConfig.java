package limacat.helloworld.configuration;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import limacat.helloworld.JobCompletionNotificationListener;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    // tag::readerwriterprocessor[]
    @Bean
    public ItemReader<Integer> reader() {   	
        return new ItemReader<Integer>() {

        	private final int max = 1000;
        	
        	private AtomicInteger current = new AtomicInteger(0);
        	
			@Override
			public Integer read()
					throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
				int result = current.getAndIncrement();
				
				if (result < max) {
					return result;
				}

				return null;
			}
        	
        };
    }

//    @Bean
//    public PersonItemProcessor processor() {
//        return new PersonItemProcessor();
//    }

    @Bean
    public ItemWriter<Integer> writer(DataSource dataSource) {
        return new  ItemWriter<Integer>() {

			@Override
			public void write(List<? extends Integer> items) throws Exception {
				for (Integer item : items) {
					System.out.println(item);
				}
			}
        	
        };
//        JdbcBatchItemWriterBuilder<Person>()            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
//            .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
//            .dataSource(dataSource)
//            .build();
    }
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(step1)
            .end()
            .build();
    }

    @Bean
    public Step step1(ItemWriter<Integer> writer) {
        return stepBuilderFactory.get("step1")
            .<Integer, Integer> chunk(10)
            .reader(reader())
            .writer(writer)
            .build();
    }
    // end::jobstep[]
}