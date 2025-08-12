package org.example;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.ConnectConfig;

public class Connect {

    public static void main(String[] args) {
        String CLUSTER_ENDPOINT = "https://in03-110c9f11e0c1e40.serverless.ali-cn-hangzhou.cloud.zilliz.com.cn";
        String TOKEN = "xx";
// A valid token could be either
// - An API key, or
// - A colon-joined cluster username and password, as in `user:pass`

        // 1. Connect to Milvus server
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build();

        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        System.out.println("Connected to " + CLUSTER_ENDPOINT);
    }



}
