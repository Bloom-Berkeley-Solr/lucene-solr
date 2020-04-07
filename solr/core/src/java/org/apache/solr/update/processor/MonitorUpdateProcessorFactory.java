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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.search.Query;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.handler.QueryRegisterHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.jose4j.json.JsonUtil;
import org.jose4j.lang.JoseException;

public class MonitorUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private class MonitorWatcher implements Watcher {

    SolrQueryRequest req;

    MonitorWatcher(SolrQueryRequest req) {
      this.req = req;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      syncQueries(req);
    }
  }

  private class ZkReconnectListener implements OnReconnect {

    SolrQueryRequest req;

    ZkReconnectListener(SolrQueryRequest req) {
      this.req = req;
    }

    @Override
    public void command() {
      syncQueries(req);
    }
  }

  public static final String zkQueryPath = "/monitor.json";
  // public static final String zkParentPath = "/monitor";
  public static final String parserDefaultField = "defaultField";

  private static Monitor singletonMonitor = null;

  // use this hash map to track all local queries, can be used to check the difference during synchronization
  private HashMap<String, Long> queryVersion = new HashMap<>();

  private SolrZkClient client;

  public static Monitor getMonitor() {
    return singletonMonitor;
  }


  // TODO: which parser to use

  /**
   * @param queryString the input query string
   * @param req         the corresponding reconstructed SolrQueryRequest (from params)
   * @return the parsed query object
   */
  public static Query parse(String queryString, SolrQueryRequest req) {
    try {
      String deyType = req.getParams().get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE);
      QParser parser = QParser.getParser(queryString, deyType, req);
      return parser.getQuery();
    } catch (SyntaxError | FuzzyTermsEnum.FuzzyTermsException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
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
      zc.addOnReconnectListener(new ZkReconnectListener(req));
      syncQueries(req);
    } catch (IOException ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }

  // FIXME: is it thread-safe?
  synchronized void syncQueries(SolrQueryRequest req) {
    try {
      // TODO: watcher x 2? may be redundant
      if (client.exists(zkQueryPath, new MonitorWatcher(req), true) == null) return;
      byte[] bytesRead = client.getData(zkQueryPath, new MonitorWatcher(req), null, true);
      String jsonStr;
      if (bytesRead == null)
        jsonStr = "{}";
      else
        jsonStr = new String(bytesRead);
      Map<String, Object> jsonMap = JsonUtil.parseJson(jsonStr);

      Monitor monitor = getMonitor();
      ArrayList<MonitorQuery> queries = new ArrayList<>();

      for (String queryId : jsonMap.keySet()) {

        // read and deserialize values from Zk
        Object value = jsonMap.get(queryId);
        // TODO: maybe support other data types than String, e.g. boolean, int, float, ...
        if (!(value instanceof Map))
          throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Only support String as values in monitor queries json configuration");
        Map<String, Object> queryNodeMap = (Map) value;

        // read from zk
        Long version = (Long) queryNodeMap.get(QueryRegisterHandler.ZK_KEY_VERSION);
        // check if this query already exist in local monitor (with latest version)
        if (queryVersion.containsKey(queryId) && version.equals(queryVersion.get(queryId))) continue;
        queryVersion.put(queryId, version);

        String queryString = (String) queryNodeMap.get(QueryRegisterHandler.ZK_KEY_QUERY_STRING);
        String paramsStr = (String) queryNodeMap.get(QueryRegisterHandler.ZK_KEY_SOLR_PARAMS);

        // deserialize params
        byte[] paramBytes = Base64.base64ToByteArray(paramsStr);
        SolrParams params = new MultiMapSolrParams((Map) getObject(paramBytes));

        // reconstruct SolrQueryRequest
        SolrQueryRequest reconstructedReq = new SolrQueryRequestBase(req.getCore(), params) {
        };
        queries.add(new MonitorQuery(queryId, parse(queryString, reconstructedReq)));
      }

      monitor.register(queries);
    } catch (KeeperException | InterruptedException | JoseException | IOException ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }

  private static Object getObject(byte[] bytes) throws IOException {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(new ByteArrayInputStream(bytes));
    }
  }
}
