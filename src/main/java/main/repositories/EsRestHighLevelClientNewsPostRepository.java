package main.repositories;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.NewsPost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class EsRestHighLevelClientNewsPostRepository {
    private static final String INDEX = "news";
    private static final String TYPE = "doc";
    private final RestHighLevelClient client;

    @Autowired
    private ObjectMapper objectMapper;
    private static final Logger LOG = LoggerFactory.
            getLogger(EsRestHighLevelClientNewsPostRepository.class);

    public EsRestHighLevelClientNewsPostRepository(RestHighLevelClient client){
        this.client = client;
    }

    public void save(NewsPost newsPost) throws IOException {

        final IndexRequest indexRequest = new IndexRequest(INDEX, TYPE);

        final String json = objectMapper.writeValueAsString(newsPost);
        indexRequest.source(json, XContentType.JSON);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    public void save(List<NewsPost> newsPosts) throws IOException {

        if(newsPosts == null || newsPosts.isEmpty()){
            LOG.warn("Attempt to perform bulk save on none entities");
            return;
        }

        final BulkRequest bulkRequest = new BulkRequest();

        newsPosts.forEach(e -> {
            try {
                bulkRequest.add(getIndexRequest(e));
            } catch (JsonProcessingException ex) {
                LOG.error("Error occured while creating a bulk request: ", ex);
            }
        });

        try {
            final BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                LOG.error("Unexpected failure during bulk saving: {}", bulkResponse.buildFailureMessage());
            }
        } catch (final IOException e) {
            LOG.error("During bulk saving entities", e);
            throw e;
        }

    }

    private IndexRequest getIndexRequest(final NewsPost newsPost) throws JsonProcessingException {
        final String json = objectMapper.writeValueAsString(newsPost);
        return new IndexRequest(INDEX, TYPE)
                .source(json, XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
    }

    public SearchResponse search(final SearchSourceBuilder searchSourceBuilder) throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.types(TYPE);
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

}
