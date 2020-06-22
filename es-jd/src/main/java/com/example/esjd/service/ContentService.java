package com.example.esjd.service;

import com.alibaba.fastjson.JSON;
import com.example.esjd.pojo.Content;
import com.example.esjd.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 业务编写
 */
@Service
public class ContentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    // 1.解析数据放入es库中
    public Boolean parseContent(String keywords) throws IOException {
        // 获取要搜索的内容
        List<Content> contents = new HtmlParseUtil().parseJD(keywords);

        // 把从京东上爬下来的数据存到es库中
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");
        for (int i = 0; i < contents.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("jd_goods")
                    .source(JSON.toJSONString(contents.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        return !bulkResponse.hasFailures();
    }

    // 2.获取数据，实现基本的搜索功能
    public List<Map<String, Object>> searchPage(String keyword, int pageNo, int pageSize) throws IOException {
        if (pageNo <= 1){
            pageNo = 1;
        }

        // 条件搜索
        SearchRequest request = new SearchRequest("jd_goods");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        //精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 执行搜索
        request.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        // 解析结果
        ArrayList<Map<String, Object>> mapArrayList = new ArrayList<>();
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            mapArrayList.add(documentFields.getSourceAsMap());
        }

        return mapArrayList;
    }

    // 3.获取数据，实现高亮搜索功能
    public List<Map<String, Object>> searchPageHighLight(String keyword, int pageNo, int pageSize) throws IOException {
        if (pageNo <= 1){
            pageNo = 1;
        }

        // 条件搜索
        SearchRequest request = new SearchRequest("jd_goods");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);


        //精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        // 多个高亮显示
        highlightBuilder.requireFieldMatch(false);
        searchSourceBuilder.highlighter(highlightBuilder);

        // 执行搜索
        request.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        // 解析结果
        ArrayList<Map<String, Object>> mapArrayList = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            // 解析高亮的字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            // 原来的结果
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            // 解析高亮的字段，将原来的字段替换为我们的高亮的字段即可！
            if (title != null){
                Text[] fragments = title.fragments();
                String n_title = "";
                for (Text text : fragments) {
                    n_title += text;
                }
                // 高亮的字段替换原来的内容
                sourceAsMap.put("title", n_title);
            }
            mapArrayList.add(sourceAsMap);
        }

        return mapArrayList;
    }
}
