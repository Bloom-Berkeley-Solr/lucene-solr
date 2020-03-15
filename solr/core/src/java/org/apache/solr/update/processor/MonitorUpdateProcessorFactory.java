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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.jose4j.json.JsonUtil;
import org.jose4j.lang.JoseException;

public class MonitorUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private class MonitorWatcher implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
      syncQueries();
    }
  }

  private class ZkReconnectListener implements OnReconnect {

    @Override
    public void command() throws KeeperException.SessionExpiredException {
      syncQueries();
    }
  }

  public static final String zkQueryPath = "/monitor.json";
  // public static final String zkParentPath = "/monitor";
  public static final String parserDefaultField = "defaultField";

  private static Monitor singletonMonitor = null;

  private SolrZkClient client;

  public static Monitor getMonitor() {
    return singletonMonitor;
  }


  // TODO: which parser to use
  public static Query parse(String query) {
    StandardQueryParser parser = new StandardQueryParser();
    try {
      return parser.parse(query, parserDefaultField);
    } catch (QueryNodeException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    if (singletonMonitor == null)
      createMonitorInstance(req);

    return new MonitorUpdateProcessor(req, rsp, next);
  }

  void createMonitorInstance(SolrQueryRequest req) {
    // TODO: does it need to be thread safe?
    try {
      // TODO: use which analyzer and presearcher?
      singletonMonitor = new Monitor(new StandardAnalyzer(), Presearcher.NO_FILTERING);
      ZkController zc = req.getCore().getCoreContainer().getZkController();
      this.client = zc.getZkClient();
      zc.addOnReconnectListener(new ZkReconnectListener());
      syncQueries();
    } catch (IOException ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }

  void syncQueries() {
    try {
      if (client.exists(zkQueryPath, new MonitorWatcher(), true) == null) return;
      byte[] bytesRead = client.getData(zkQueryPath, new MonitorWatcher(), null, true);
      String jsonStr;
      if (bytesRead == null)
        jsonStr = "{}";
      else
        jsonStr = new String(bytesRead);
      Map<String, Object> jsonMap = JsonUtil.parseJson(jsonStr);

      Monitor monitor = getMonitor();
      // TODO: clear and add is inefficient, consider use a smarter method
      monitor.clear();
      ArrayList<MonitorQuery> queries = new ArrayList<MonitorQuery>();

      for (String queryId : jsonMap.keySet()) {
        Object value = jsonMap.get(queryId);
        // TODO: maybe support other data types than String, e.g. boolean, int, float, ...
        if (!(value instanceof String))
          throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Only support String as values in monitor queries json configuration");
        String query = (String) value;
        queries.add(new MonitorQuery(queryId, parse(query)));
      }

      monitor.register(queries);
    } catch (KeeperException | InterruptedException | JoseException | IOException ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }
}
