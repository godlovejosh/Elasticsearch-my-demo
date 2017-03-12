package com.deppon.cubc.trade.search.core.service;


import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wuxing
 */
public class ESTools {
    private static final String CLUSTER_NAME = "cubc";
    private static final String ADDRESS = "10.230.27.185:9300;10.230.27.186:9300";
    private static final String INDEX_NAME = "wuxing_test";
    private static final String TYPE_NAME = "wuxing";

    public static void main(String[] args) {
        TransportClient client = null;
        try {
            client = ESClientManager.getConnection(CLUSTER_NAME, ADDRESS);
//            deleteIndex(client, INDEX_NAME);
//            createIndex(client, INDEX_NAME);
//            createMapping(client, INDEX_NAME, TYPE_NAME);
//            insertData(client, INDEX_NAME, TYPE_NAME, 30);
//            deleteOneDoc(client, INDEX_NAME, TYPE_NAME, "1");

//            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//            boolQueryBuilder.must(QueryBuilders.termQuery("name", "bggg"));
//            ResultList<WuxingDO> resultList = new ResultList<>();
//            SearchResultList<WuxingDO> searchRSList = search(client, INDEX_NAME, TYPE_NAME, boolQueryBuilder, 1, 30, "name", "desc", WuxingDO.class);
//            resultList.setTotal(searchRSList.getTotal());
//            resultList.setDatalist(searchRSList.getData());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    /**
     * 判断索引是否存在
     *
     * client.admin().indices().prepareExists(indexName).execute().actionGet();
     * @param indexName
     * @return
     */
    public static Boolean existsIndex(TransportClient client, String indexName) {
        IndicesExistsRequestBuilder indicesExistsRequestBuilder = client.admin()
                .indices()
                .prepareExists(indexName);
        IndicesExistsResponse indicesExistsResponse = indicesExistsRequestBuilder.execute()
                .actionGet();
        return indicesExistsResponse.isExists();
    }

    /**
     * 创建索引
     *
     * client.admin().indices().prepareCreate(indexName).get();
     * @param indexName
     */
    public static Boolean createIndex(TransportClient client, String indexName) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin()
                .indices()
                .prepareCreate(indexName);
        CreateIndexResponse indexResponse = createIndexRequestBuilder.get();
        return indexResponse.isAcknowledged();
    }

//    public void createIndex(String indexName) {
//        client.admin().indices().create(new CreateIndexRequest(indexName))
//                .actionGet();
//    }

    /**
     * 删除索引
     *
     * client.admin().indices().prepareDelete(indexName).execute().actionGet();
     * @param indexName
     * @return
     */
    public static Boolean deleteIndex(TransportClient client, String indexName) {
        if (existsIndex(client, indexName)) {
            DeleteIndexRequestBuilder deleteIndexRequestBuilder = client.admin()
                    .indices()
                    .prepareDelete(indexName);
            DeleteIndexResponse deleteIndexResponse  = deleteIndexRequestBuilder.execute()
                    .actionGet();
            return deleteIndexResponse.isAcknowledged();
        }
        return Boolean.TRUE;
    }

    /**
     * 创建mapping
     *
     * @param indexName
     * @param typeName
     */
    public static Boolean createMapping(TransportClient client, String indexName, String typeName) {
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject()
                    .startObject(typeName)
                    .startObject("properties")
                    .startObject("address").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("hello").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            PutMappingRequest putMappingRequest = Requests.putMappingRequest(indexName)
                    .type(typeName)
                    .source(mapBuilder);
            PutMappingResponse putMappingResponse = client.admin().indices().putMapping(putMappingRequest).actionGet();
            return putMappingResponse.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /**
     * 批量造数据
     * @param client
     * @param indexName
     * @param typeName
     * @param count
     */
    public static void insertData(TransportClient client, String indexName, String typeName, int count) {
        Map<String, String> map = new HashMap<>();
        map.put("name", "bggg");
        map.put("hello", "你好");
        map.put("address", "地址");

        String strObj = JSON.toJSONString(map);

        Boolean result = null;
        for (int i = 0; i < count; i++) {
            UpdateResponse response = client.prepareUpdate(indexName, typeName, String.valueOf(i))
                    .setDoc(strObj)
                    .setUpsert(strObj)
                    .execute()
                    .actionGet();
            ActionWriteResponse.ShardInfo shardInfo = response.getShardInfo();
//            int failed = shardInfo.getFailed();
//            if (failed == 0) {
//                result = Boolean.TRUE;
//            } else {
//                result = Boolean.FALSE;
//            }
        }
    }

    /**
     *
     * client.prepareGet(index, type, id).get().isExists()
     * @param client
     * @param indexName
     * @param typeName
     * @param id
     * @return
     */
    public static Boolean existsDoc(TransportClient client, String indexName, String typeName, String id) {
        GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, typeName, id);
        GetResponse response = getRequestBuilder.get();
        return response.isExists();
    }

    /**
     * 删除指定Index Type对应的文档(一行记录)
     *
     * client.prepareDelete(indexName, typeName, id).execute().actionGet().isFound();
     * @param indexName
     * @return
     */
    public static Boolean deleteOneDoc(TransportClient client, String indexName, String typeName, String id) {
        if (existsDoc(client, indexName, typeName, id)) {
            DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(indexName, typeName, id);
            DeleteResponse response = deleteRequestBuilder.execute()
                    .actionGet();
            return response.isFound();
        }
        return Boolean.TRUE;
    }

    /**
     *
     * client.prepareSearch(indexName).setTypes(typeName).setQuery(boolQueryBuilder).setExplain(true);
     * @param client
     * @param indexName
     * @param typeName
     * @param ids
     * @return
     */
    public String deleteDocByIds(TransportClient client, String indexName, String typeName, String... ids) {
        String result = "";
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (String id : ids) {
            DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(indexName, typeName, id);
            bulkRequestBuilder.add(deleteRequestBuilder);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            result = bulkResponse.buildFailureMessage();
        }
        return result;
    }

    public static <R> SearchResultList<R> search(TransportClient client, String indexName, String typeName, BoolQueryBuilder boolQueryBuilder,
                                                 Integer currentPage, Integer pageSize, String sortField, String sortType, Class<R> rCls) {

        pageSize = (null == pageSize || pageSize.intValue() <= 0) ? 20 : pageSize;
        currentPage = (null == currentPage || currentPage.intValue() <= 1) ? 0 : Integer.valueOf((currentPage.intValue() - 1) * pageSize.intValue());
        sortField = true == StringUtils.isBlank(sortField) ? "modify_date" : sortField;
        sortType = true == StringUtils.isBlank(sortType) ? "desc" : sortType;
        SortOrder sortOrder = sortType.equals("desc") ? SortOrder.DESC : SortOrder.ASC;

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(boolQueryBuilder)
                .setFrom(currentPage.intValue())
                .setSize(pageSize.intValue())
                .addSort(sortField, sortOrder)
                .setExplain(true);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchResultList<R> searchRSList = convert(rCls, searchResponse);
        return searchRSList;
    }

    public static <R> SearchResultList<R> convert(Class<R> rCls, SearchResponse searchResponse) {
        SearchResultList<R> searchResultList = new SearchResultList<>();
        List<R> result = new ArrayList<>();
        searchResultList.setTotal(searchResponse.getHits().getTotalHits());

        for (SearchHit searchHit : searchResponse.getHits()) {
            R r = JSON.parseObject(searchHit.getSourceAsString(), rCls);
            result.add(r);
        }
        searchResultList.setData(result);

        if (null != searchResponse.getAggregations()) {
            List<Aggregation> aggregationList = searchResponse.getAggregations().asList();
            searchResultList.setAggData(aggregationList);
        }
        return searchResultList;
    }

    public static String toJson(Object object) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    static class ESClientManager {

        private ESClientManager() {}

        /**
         * build TransportClient
         *
         * @see TransportClient
         */
        public static TransportClient getConnection(String clusterName, String addressesStr) {
            return TransportClient
                    .builder()
                    .settings(settings(clusterName))
                    .build()
                    .addTransportAddresses(tranAddress(addressesStr));
        }

        /**
         * build {@link Settings}
         */
        private static Settings settings(String clusterName) {
            return Settings.settingsBuilder()
                    .put("cluster.name", clusterName)// 集群名称
                    .put("connector.transport.sniff", Boolean.TRUE) // 在局域网下嗅探，自主组成集群
                    .build();
        }

        /**
         * build {@link TransportAddress}
         *
         * @return TransportAddress数组
         * @see TransportAddress
         */
        private static TransportAddress[] tranAddress(String addressesStr) {

            List<TransportAddress> transportAddressesList = new ArrayList<>();
            /*分号分割*/
            String[] addressesArr =
                    addressesStr.split(";");
            TransportAddress[] result = new TransportAddress[addressesArr.length];

            for (String str : addressesArr) {
                /*冒号分割*/
                String[] ipAddr =
                        str.split(":");
                try {
                    transportAddressesList.add(
                            new InetSocketTransportAddress(
                                    InetAddress.getByName(
                                            String.valueOf(ipAddr[0])), Integer.valueOf(ipAddr[1])));

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }

            transportAddressesList.toArray(result);
            return result;
        }
    }

    static class SearchResultList<R> {
        private List<R> data;
        private List<Aggregation> aggData;
        private Long total;
        private Integer currentPage;
        private Integer pageSize;

        public List<R> getData() {
            return data;
        }
        public List<Aggregation> getAggData() {
            return aggData;
        }

        public void setAggData(List<Aggregation> aggData) {
            this.aggData = aggData;
        }

        public void setData(List<R> data) {
            this.data = data;
        }

        public Long getTotal() {
            return total;
        }

        public void setTotal(Long total) {
            this.total = total;
        }

        public Integer getCurrentPage() {
            return currentPage;
        }

        public void setCurrentPage(Integer currentPage) {
            this.currentPage = currentPage;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public void setPageSize(Integer pageSize) {
            this.pageSize = pageSize;
        }
    }

    static class WuxingDO implements Serializable {

        private static final long serialVersionUID = 3574996674491990917L;

        private String address;
        private String hello;
        private String name;

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getHello() {
            return hello;
        }

        public void setHello(String hello) {
            this.hello = hello;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

}