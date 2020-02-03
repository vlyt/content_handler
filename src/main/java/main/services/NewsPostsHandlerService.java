package main.services;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.NewsPost;
import main.repositories.EsRestHighLevelClientNewsPostRepository;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class NewsPostsHandlerService {

    @Autowired
    private SqsService sqsService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EsRestHighLevelClientNewsPostRepository repository;

    private static final Logger LOG = LoggerFactory.
            getLogger(NewsPostsHandlerService.class);


    private SearchSourceBuilder createBuilder(){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder qB = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("header", "cat3"));

        TermQueryBuilder qB2 = QueryBuilders.termQuery("body", "stranger");

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("by_header").field("header")
                .subAggregation(AggregationBuilders.terms("by_dateTime").field("dateTime").order(BucketOrder.count(false)));

        searchSourceBuilder.query(qB2);
        searchSourceBuilder.aggregation(termsAggregationBuilder);


        return searchSourceBuilder;
    }


    @Scheduled(cron = "*/1 * * * * *")
    public void run() throws IOException {
        List<Message> messages = sqsService.receiveMessages();
            if(messages != null && !messages.isEmpty()){
                messages.forEach(m -> {
                    try {
                        NewsPost newsPost = objectMapper.readValue(m.getBody(), NewsPost.class);
                        repository.save(newsPost);

                    } catch (final IOException exc) {
                        LOG.error("While converting json to object", exc);
                    }
                    sqsService.deleteMessage(m);
                });

            }
        }




}
