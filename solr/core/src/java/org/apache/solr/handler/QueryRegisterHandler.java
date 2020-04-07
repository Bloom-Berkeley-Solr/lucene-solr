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

package org.apache.solr.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.MonitorUpdateProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.jose4j.json.JsonUtil;
import org.jose4j.lang.JoseException;

public class QueryRegisterHandler extends RequestHandlerBase implements SolrCoreAware {

  public static final String PARSER_DEFAULT_FIELD_NAME = "defaultField";
  public static final String PARAM_QUERY_ID_NAME = "id";
  public static final String PARAM_COLLECTION_NAMES = "collections";
  public static final String ZK_KEY_SOLR_PARAMS = "params";
  public static final String ZK_KEY_QUERY_STRING = "q";
  public static final String ZK_KEY_VERSION = "version";

  private SolrCore core;

  static Query parse(String query) {
    try {
      StandardQueryParser parser = new StandardQueryParser();
      return parser.parse(query, PARSER_DEFAULT_FIELD_NAME);
    } catch (QueryNodeException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CoreContainer cc = req.getCore().getCoreContainer();
    SolrZkClient client = cc.getZkController().getZkClient();
    SolrParams params = req.getParams();
    String queryId = params.get(PARAM_QUERY_ID_NAME);

    // use query component to parse query string
    List<SearchComponent> components = new ArrayList<>();
    components.add(core.getSearchComponent("query"));
    ResponseBuilder rb = new ResponseBuilder(req, rsp, components);
    for (SearchComponent c : components) c.prepare(rb);
    Query query = rb.getQuery();

    // serialize SolrParams
    byte[] paramBytes = getBytes(params);
    String paramString = Base64.byteArrayToBase64(paramBytes);

    registerQueryToZk(client, queryId, query, paramString);
  }

  /**
   * Write the query to Zookeeper.
   * All queries are stored in a single node, so read -> modify -> write.
   * "a single handler instance is reused for all relevant queries" -> synchronized method
   *
   * @param client      Zookeeper client
   * @param queryId     The user specified id of query
   * @param query       Parsed Lucene {@link Query} object
   * @param paramString The string(base64) representation of the serialized query
   * @throws KeeperException      If error occurred related to Zookeeper
   * @throws InterruptedException
   * @throws JoseException        If error occurred in JsonUtil
   */
  synchronized private void registerQueryToZk(SolrZkClient client, String queryId, Query query, String paramString) throws KeeperException, InterruptedException, JoseException {
    String path = MonitorUpdateProcessorFactory.zkQueryPath;
    if (!client.exists(path, true)) {
      client.makePath(path, true);
    }

    // read & prepare data
    byte[] bytesRead = client.getData(path, null, null, true);
    String jsonStr = "{}";
    if (bytesRead != null)
      jsonStr = new String(bytesRead, StandardCharsets.UTF_8);
    long version = 0;
    Map<String, Object> oldJsonMap = JsonUtil.parseJson(jsonStr);
    if (oldJsonMap.containsKey(queryId)) {
      version = (Long) ((Map) oldJsonMap.get(queryId)).get(ZK_KEY_VERSION) + 1;
    }

    // write data
    Map<String, Object> queryNodeMap = new HashMap<String, Object>();
    LinkedHashMap<String, Object> newJsonMap = new LinkedHashMap<>(oldJsonMap);
    queryNodeMap.put(ZK_KEY_QUERY_STRING, query.toString());
    queryNodeMap.put(ZK_KEY_SOLR_PARAMS, paramString);
    queryNodeMap.put(ZK_KEY_VERSION, version);
    newJsonMap.put(queryId, queryNodeMap);
    client.setData(path, JsonUtil.toJson(newJsonMap).getBytes(StandardCharsets.UTF_8), true);
  }


  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }

  /**
   * Serialize an object to a byte array using {@link JavaBinCodec}
   *
   * @param o The object to serialize
   * @return A byte array
   * @throws IOException
   */
  private static byte[] getBytes(Object o) throws IOException {
    try (JavaBinCodec javabin = new JavaBinCodec(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      javabin.marshal(o, baos);
      return baos.toByteArray();
    }
  }
}
