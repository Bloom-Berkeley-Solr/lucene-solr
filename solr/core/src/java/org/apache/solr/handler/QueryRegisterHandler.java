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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.MonitorQuerySerializer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.MonitorUpdateProcessorFactory;

public class QueryRegisterHandler extends RequestHandlerBase {

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
    MonitorQuerySerializer serializer = MonitorQuerySerializer.fromParser(QueryRegisterHandler::parse);
    MonitorQuery mq = new MonitorQuery(queryId, null, q, new HashMap<>());

    String collection = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
    String path = MonitorUpdateProcessorFactory.zkParentPath + '/' + collection + '/' + queryId;
    if (!client.exists(path, true)) {
      client.makePath(path, true);
    }
    byte[] bytes = serializer.serialize(mq).bytes;
    client.setData(path, bytes, true);
  }

  @Override
  public String getDescription() {
    return null;
  }
}
