package com.luakun;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luakun.pojo.Article;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: luakun
 * @Date: 2021/03/17/15:50
 * @Description:
 */

@SpringBootTest
@RunWith(SpringRunner.class)
public class EsTest {

    @Autowired
    private TransportClient transportClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void createIndexAndDocument(){

    }
/**
 * 索引 index
 * 文档 document
 * 字段 filed
 * 映射 mapping
 */
    /**
     * 创建索引
     */
    @Test
    public void createIndex() {
        //准备创建索引 ，指定索引名 执行创建的动作（get方法）
        transportClient.admin().indices().prepareCreate("blog03").get();
        transportClient.admin().indices().prepareCreate("hello").get();
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex(){
        transportClient.admin().indices().prepareDelete("blog01").get();
    }
    /**
     * 创建映射
     */

    @Test
    public void putMapping() throws Exception {
        //创建索引 存在.删除..创建
        transportClient.admin().indices().prepareCreate("blog01").get();

        //创建映射

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                    .startObject("id")
                        .field("type", "long")
                        .field("store", "true")
                    .endObject()
                    .startObject("title")
                        .field("type", "text")
                        .field("analyzer", "ik_smart")
                        .field("index", "true")
                        .field("store", "true")
                    .endObject()
                    .startObject("content")
                        .field("type", "text")
                        .field("analyzer", "ik_smart")
                        .field("index", "true")
                        .field("store", "true")
                    .endObject()
                .endObject()
                .endObject()
                .endObject();
        PutMappingRequest putMappingRequest = new PutMappingRequest("blog01").type("article")
                .source(builder);
        transportClient.admin().indices().putMapping(putMappingRequest).get();

    }

    /**
     * 创建文档 更新文档 使用的Ik分词器
     */
    @Test
    public void creatIndexAndDocument() throws Exception{
        //设置数据
        Article article = new Article();
        article.setTitle("华为P40 真的很好用呢");
        article.setContent("华为P40 真真真真真真真的非常好用 价格实惠 还是 5G");
        article.setId(1L);
        IndexResponse indexResponse =
                transportClient.prepareIndex("blog05","article","1")
                        .setSource(objectMapper.writeValueAsString(article), XContentType.JSON)
                        .get();
        System.out.println(indexResponse);
    }

    @Test
    public void createDocument() throws Exception {
        //构建批量操作桶子 BulkRequestBuilder
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        long startTime = System.currentTimeMillis();
        for (Long i = 1L; i <= 100000; i++) {
            Article article = new Article();
            article.setId(i);
            article.setContent("华为大法好 用了就成仙,,嘀哩嘀哩嘀哩嘀哩,哈哈" + i );
            article.setTitle("华为手机啊" + i);
            String json = objectMapper.writeValueAsString(article);
            IndexRequest indexRequest = new IndexRequest("blog01","article"," "+i)
                    .source(json,XContentType.JSON);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        long endTime = System.currentTimeMillis();
        System.out.println("一共消耗了:" + (endTime - startTime) + "毫秒");
        System.out.println("获取状态：" + bulkItemResponses.status());
        if (bulkItemResponses.hasFailures()) {
            System.out.println("还有些--->有错误");
        }
    }

}
