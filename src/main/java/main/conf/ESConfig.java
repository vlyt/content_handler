package main.conf;

import main.repositories.EsRestHighLevelClientNewsPostRepository;
import main.services.NewsPostsHandlerService;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

@Configuration

@EnableElasticsearchRepositories(basePackages = "main.main.repositories")
public class ESConfig {

    @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.port}")
    private int port;

    @Value("${elasticsearch.protocol}")
    private String protocol;

    @Bean
    public EsRestHighLevelClientNewsPostRepository getRepository(){
        return new EsRestHighLevelClientNewsPostRepository(getEsClient());
    }


    @Bean
    public RestHighLevelClient getEsClient(){
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, protocol)));
    }

    @Bean
    public NewsPostsHandlerService getNewsPostsHandler(){
        return new NewsPostsHandlerService();
    }

}
