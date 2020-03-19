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

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.MonitorUpdateProcessorFactory;
import org.jose4j.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRegisterHandler extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Query parse(String query) {
    try {
      StandardQueryParser parser = new StandardQueryParser();
      return parser.parse(query, "defaultField");
    } catch (QueryNodeException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CoreContainer cc = req.getCore().getCoreContainer();
    SolrZkClient client = cc.getZkController().getZkClient();
    SolrParams params = req.getParams();
    String q = params.get("q");
    String queryId = params.get("id");
    // FIXME: convert to Lucene Query

    // String collection = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
    String path = MonitorUpdateProcessorFactory.zkQueryPath;


    // FIXME: concurrent modification? do we need a lock to protect it
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
      Map<String, Object> jsonMap = JsonUtil.parseJson(jsonStr);
      // current behavior: overwrite, since Zookeeper guarantees atomic writes
      LinkedHashMap<String, Object> queryMap = new LinkedHashMap<>(jsonMap);
      queryMap.put(queryId, q);
      client.setData(path, JsonUtil.toJson(queryMap).getBytes(), true);
    }
  }


  @Override
  public String getDescription() {
    return null;
  }
}
