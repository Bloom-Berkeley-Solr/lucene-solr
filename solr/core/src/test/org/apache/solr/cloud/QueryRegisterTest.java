/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.processor.MonitorUpdateProcessorFactory;
import org.jose4j.json.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert.*;

public class QueryRegisterTest extends SolrCloudTestCase{

  SolrZkClient zkClient;
  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("shareSchema", "true");  // see testSharedSchema

    configureCluster(1) // some tests here assume 1 node
        .addConfig("conf1",configset("cloud-minimal-query"))
        .configure();
  }

  @Test
  public void testQueryRegister() throws Exception{
    String zkPath= MonitorUpdateProcessorFactory.zkQueryPath;
    zkClient=zkClient();
    zkClient.makePath(zkPath,false);
    assertNull(zkClient.getData(zkPath,null,null,false));
    String baseUrl=cluster.getJettySolrRunner(0).getBaseUrl().toString();

    HttpSolrClient solr = new HttpSolrClient.Builder(baseUrl).build();

    ModifiableSolrParams params=new ModifiableSolrParams();
    params.set("id",1);
    params.set("q","name:peter");
    GenericSolrRequest request=new GenericSolrRequest(SolrRequest.METHOD.POST,"/register",params);
    request.setResponseParser(new NoOpResponseParser("json"));
    CollectionAdminRequest.createCollection("test_query_register", "conf1", 1, 1)
        .processAndWait(solr, DEFAULT_TIMEOUT);
    NamedList<Object> nl=solr.request(request,"test_query_register");
    String s1=(String)nl.get("response");
    Map<String,Object> map=JsonUtil.parseJson(s1);
    Map<String,Long> resp=(Map)map.get("responseHeader");



    Assert.assertEquals((Long)resp.get("status"),(Long)0L);

    byte[] bytesRead = zkClient.getData(zkPath, null, null, true);
    Map<String,Object> m=JsonUtil.parseJson(new String(bytesRead));
    assertTrue(m.keySet().contains("1"));
    assertEquals((String)m.get("1"),"name:peter");

    solr.close();


  }
  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections();
    zkClient.close();
    System.out.println(1);
  }
}
