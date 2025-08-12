package org.example;

//import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.response.DeleteResp;

public class CollectionDemo {

    private static final Logger log = Logger.getLogger(CollectionDemo.class);

    public static void main(String[] args){
//        deleteEntity();
//        addData();
        search();
    }

    public static void search(){
        MilvusClientV2 client = getClient();

        FloatVec queryVector = new FloatVec(new float[]{0.9858251f,-0.81446517f,0.6299267f,0.12069069f,-0.14462778f});
        SearchReq searchReq = SearchReq.builder()
                .collectionName("custom_setup")
                .data(Collections.singletonList(queryVector))
                .outputFields(Collections.singletonList("my_id"))
                .topK(2)
                .build();

        SearchResp searchResp = client.search(searchReq);

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            System.out.println("TopK results:");
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    public static void deleteEntity(){
        MilvusClientV2 client = getClient();

// 13. Delete entities by IDs
        DeleteReq deleteReq = DeleteReq.builder()
                .collectionName("custom_setup")
                .ids(Arrays.asList(0L, 1L, 2L, 3L, 4L))
                .build();

        DeleteResp deleteRes = client.delete(deleteReq);

        System.out.println(JSONObject.toJSON(deleteRes));

// Output:
// {"deleteCnt": 5}
    }

//    public void searchOne(){
//        MilvusClientV2 client = getClient();
//        // 8. Search with a filter expression using schema-defined fields
//        List<List<Float>> filteredVectorSearchData = new ArrayList<>();
//        filteredVectorSearchData.add(Arrays.asList(0.041732933f, 0.013779674f, -0.027564144f, -0.013061441f, 0.009748648f));
//
//        SearchResp    searchReq = SearchReq.builder()
//                .collectionName("custom_setup")
//                .data(filteredVectorSearchData)
//                .filter("4 < id < 8")
//                .outputFields(Arrays.asList("id"))
//                .topK(3)
//                .build();
//
//        SearchResp filteredVectorSearchRes = client.search(searchReq);
//
//        System.out.println(JSONObject.toJSON(filteredVectorSearchRes));
//
//// Output:
//// {"searchResults": [[
////     {
////         "distance": 0.08821295,
////         "id": 5,
////         "entity": {"id": 5}
////     },
////     {
////         "distance": 0.074322253,
////         "id": 6,
////         "entity": {"id": 6}
////     },
////     {
////         "distance": 0.072796463,
////         "id": 7,
////         "entity": {"id": 7}
////     }
//// ]]}
//    }

    public static void addData() {
        MilvusClientV2 client = getClient();

// 4. Insert data into the collection

// 4.1. Prepare data

        List<JSONObject> insertData = Arrays.asList(
                new JSONObject(Map.of("my_id", 0L, "my_vector", Arrays.asList(0.3580376395471989f, -0.6023495712049978f, 0.18414012509913835f, -0.26286205330961354f, 0.9029438446296592f), "color", "pink_8682")),
                new JSONObject(Map.of("my_id", 1L, "my_vector", Arrays.asList(0.19886812562848388f, 0.06023560599112088f, 0.6976963061752597f, 0.2614474506242501f, 0.838729485096104f), "color", "red_7025")),
                new JSONObject(Map.of("my_id", 2L, "my_vector", Arrays.asList(0.43742130801983836f, -0.5597502546264526f, 0.6457887650909682f, 0.7894058910881185f, 0.20785793220625592f), "color", "orange_6781")),
                new JSONObject(Map.of("my_id", 3L, "my_vector", Arrays.asList(0.3172005263489739f, 0.9719044792798428f, -0.36981146090600725f, -0.4860894583077995f, 0.95791889146345f), "color", "pink_9298")),
                new JSONObject(Map.of("my_id", 4L, "my_vector", Arrays.asList(0.4452349528804562f, -0.8757026943054742f, 0.8220779437047674f, 0.46406290649483184f, 0.30337481143159106f), "color", "red_4794"))
//                new JSONObject(Map.of("my_id", 5L, "my_vector", Arrays.asList(0.985825131989184f, -0.8144651566660419f, 0.6299267002202009f, 0.1206906911183383f, -0.1446277761879955f), "color", "yellow_4222")),
//                new JSONObject(Map.of("my_id", 6L, "my_vector", Arrays.asList(0.8371977790571115f, -0.015764369584852833f, -0.31062937026679327f, -0.562666951622192f, -0.8984947637863987f), "color", "red_9392")),
//                new JSONObject(Map.of("my_id", 7L, "my_vector", Arrays.asList(-0.33445148015177995f, -0.2567135004164067f, 0.8987539745369246f, 0.9402995886420709f, 0.5378064918413052f), "color", "grey_8510")),
//                new JSONObject(Map.of("my_id", 8L, "my_vector", Arrays.asList(0.39524717779832685f, 0.4000257286739164f, -0.5890507376891594f, -0.8650502298996872f, -0.6140360785406336f), "color", "white_9381")),
//                new JSONObject(Map.of("my_id", 9L, "my_vector", Arrays.asList(0.5718280481994695f, 0.24070317428066512f, -0.3737913482606834f, -0.06726932177492717f, -0.6980531615588608f), "color", "purple_4976"))
        );

        // Convert Fastjson JSONObjects to Gson JsonObjects
        List<JsonObject> gsonJsonObjects = insertData.stream()
                .map(json -> JsonParser.parseString(json.toJSONString()).getAsJsonObject())
                .collect(Collectors.toList());

        // Print the converted objects
        gsonJsonObjects.forEach(System.out::println);
// 4.2. Insert data

        InsertReq insertReq = InsertReq.builder()
                .collectionName("custom_setup")
                .data(gsonJsonObjects)
                .build();

        InsertResp res = client.insert(insertReq);

        System.out.println(JSONObject.toJSON(res));

// Output:
// {"insertCnt": 10}
    }

    public static MilvusClientV2 getClient() {
        String CLUSTER_ENDPOINT = "https://in03-110c9f11e0c1e40.serverless.ali-cn-hangzhou.cloud.zilliz.com.cn";
        String TOKEN = "e37b6e6bb19b5c37390737dbc5cc01918ed48f908bf1df0f33cda42b3a21c6c862662201282a967f0871ffc092f099244960e039";
// A valid token could be either
// - An API key, or
// - A colon-joined cluster username and password, as in `user:pass`

        // 1. Connect to Milvus server
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build();

        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        log.info("client created");
        return client;
    }

    public void CreateCollection() {
        MilvusClientV2 client = getClient();

// 3.1 Create schema
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

// 3.2 Add fields to schema

        AddFieldReq myId = AddFieldReq.builder()
                .fieldName("my_id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build();

        schema.addField(myId);

        AddFieldReq myVector = AddFieldReq.builder()
                .fieldName("my_vector")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build();

        schema.addField(myVector);

// 3.3 Prepare index parameters
        IndexParam indexParamForIdField = IndexParam.builder()
                .fieldName("my_id")
                .indexType(IndexParam.IndexType.STL_SORT)
                .build();

        IndexParam indexParamForVectorField = IndexParam.builder()
                .fieldName("my_vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.IP)
                .build();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParamForIdField);
        indexParams.add(indexParamForVectorField);

// 3.4 Create a collection with schema and index parameters
        CreateCollectionReq customizedSetupReq = CreateCollectionReq.builder()
                .collectionName("custom_setup")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();

        client.createCollection(customizedSetupReq);

        log.info("Collection created");
    }
}
