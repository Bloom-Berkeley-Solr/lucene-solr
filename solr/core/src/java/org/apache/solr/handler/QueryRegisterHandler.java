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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.MonitorUpdateProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.jose4j.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRegisterHandler extends RequestHandlerBase implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PARSER_DEFAULT_FIELD_NAME = "defaultField";
  public static final String PARAM_QUERY_ID_NAME = "id";
  public static final String PARAM_COLLECTION_NAMES = "collections";
  public static final String ZK_KEY_SOLR_PARAMS = "params";
  // private static final String PARAM_CONFIG_NAME = "config";
  // private static final String ZK_KEY_CONFIG_OBJECT = "config";
  public static final String ZK_KEY_QUERY_STRING = "q";

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
    // String q = params.get("q");
    String queryId = params.get(PARAM_QUERY_ID_NAME);
    // FIXME: convert to Lucene Query
    // TODO: just for test, use query component to parse query string
    List<SearchComponent> components = new ArrayList<>();
    components.add(core.getSearchComponent("query"));
    ResponseBuilder rb = new ResponseBuilder(req, rsp, components);
    for (SearchComponent c : components) c.prepare(rb);
    Query query = rb.getQuery();
    // String collection = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
    String path = MonitorUpdateProcessorFactory.zkQueryPath;


    // serialize SolrParams
    // TODO: byte array is unreadable -> can we serialize to json representation
    byte[] paramBytes;
    paramBytes = getBytes(params);

    // "a single handler instance is reused for all relevant queries"
    synchronized (this) {
      if (!client.exists(path, true)) {
        client.makePath(path, true);
      }

      byte[] bytesRead = client.getData(path, null, null, true);
      String jsonStr;
      if (bytesRead == null)
        jsonStr = "{}";
      else
        jsonStr = new String(bytesRead);
      Map<String, Object> oldJsonMap = JsonUtil.parseJson(jsonStr);
      // current behavior: overwrite
      Map<String, Object> queryNodeMap = new HashMap<String, Object>();
      LinkedHashMap<String, Object> newJsonMap = new LinkedHashMap<>(oldJsonMap);
      queryNodeMap.put(ZK_KEY_QUERY_STRING, query.toString());
      queryNodeMap.put(ZK_KEY_SOLR_PARAMS, paramBytes);
      newJsonMap.put(queryId, queryNodeMap);
      client.setData(path, JsonUtil.toJson(newJsonMap).getBytes(), true);
    }
  }


  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }

  private static byte[] getBytes(Object o) throws IOException {
    try (JavaBinCodec javabin = new JavaBinCodec(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      javabin.marshal(o, baos);
      return baos.toByteArray();
    }
  }
}
