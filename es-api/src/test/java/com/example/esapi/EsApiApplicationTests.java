package com.example.esapi;

import com.alibaba.fastjson.JSON;
import com.example.esapi.pojo.User;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 高级客户端测试API
 */
@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /*索引 -》创建，查看，删除*/
    //    测试索引的创建 Request
    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("yang_index");
        // 2.客户端执行创建请求   IndicesClient，请求后获得响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    //  测试获取索引, 只能判断是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("yang_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test3");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    /*文档CRUD*/
    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("绘生3", 18);
        //创建请求
        IndexRequest yang_index = new IndexRequest("yang_index");

        //规则 put /yang_index/_doc/1
        yang_index.id("3");
        yang_index.timeout(TimeValue.timeValueSeconds(1));
        yang_index.timeout("1s");
        //将数据放入请求 json
        yang_index.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求,获取响应结果
        IndexResponse indexResponse = restHighLevelClient.index(yang_index, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());//IndexResponse[index=yang_index,type=_doc,id=1,version=1,result=created,seqNo=0,primaryTerm=1,shards={"total":2,"successful":1,"failed":0}]
        System.out.println(indexResponse.status());//CREATED
        System.out.println(indexResponse.getResult());//CREATED
    }

    //获取文档，判断是否存在 get /index/_doc/1
    @Test
    void testIsExists() throws IOException {
        GetRequest yang_index = new GetRequest("yang_index", "1");

        //不获取返回的 _source 的上下文, 可以写可以不写
        yang_index.fetchSourceContext(new FetchSourceContext(false));
        yang_index.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(yang_index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档信息 get /index/_doc/1
    @Test
    void testGetDocument() throws IOException {
        GetRequest yang_index = new GetRequest("yang_index", "1");

        GetResponse documentFields = restHighLevelClient.get(yang_index, RequestOptions.DEFAULT);

        //打印文档内容
        System.out.println(documentFields.getSourceAsString());
        //获取全部信息
        System.out.println(documentFields);
    }

    //更新文档信息 post /index/_doc/1
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("yang_index", "1");

        updateRequest.timeout("1s");
        User user = new User("绘生练ES", 22);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);

        //打印状态
        System.out.println(updateResponse.status());//OK
        System.out.println(updateResponse.getResult());//UPDATED
        System.out.println(updateRequest.doc());//index {[null][_doc][null], source[{"age":22,"name":"绘生练ES"}]}
    }

    //删除文档信息 delete /index/_doc/1
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("yang_index", "3");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.status());
    }

    //   ** 批量插入、更新、删除数据 **
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userArrayList = new ArrayList<>();
        userArrayList.add(new User("zhangsan1",1));
        userArrayList.add(new User("zhangsan2",2));
        userArrayList.add(new User("zhangsan3",3));
        userArrayList.add(new User("lisi1",1));
        userArrayList.add(new User("lisi1",2));
        userArrayList.add(new User("lisi1",3));

        //批处理请求
        for (int i = 0; i < userArrayList.size(); i++) {
            //批量更新和批量删除，就在这里修改对应的请求就可以了
            bulkRequest.add(
                    new IndexRequest("yang_index")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(userArrayList.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(!bulkResponse.hasFailures()); //是否成功
    }

    //查询多条数据
    /*
        SearchRequest   搜索请求
        SearchSourceBuilder     条件构造
        HighlightBuilder    构造高亮
        TermQueryBuilder    精确查询
        MatchAllQueryBuilder    匹配所有
        xxxQueryBuilder     对应我们刚才看到的命令
     */
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("yang_index");

        //构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询条件，我们可以使用 QueryBuilders 工具来实现
        // QueryBuilders.termQuery精确查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "lisi1");
        // QueryBuilders.matchAllQuery 匹配所有
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(termQueryBuilder);

        searchSourceBuilder.from();//有默认参数，可以不写
        searchSourceBuilder.size();//有默认参数，可以不写
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("==================");
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }

}
