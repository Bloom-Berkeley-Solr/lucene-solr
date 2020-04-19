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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.solr.handler.MonitorQueryRegisterHandler;
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

  private Monitor singletonMonitor = null;

  // use this hash map to track all local queries, can be used to check the difference during synchronization
  private HashMap<String, Long> queryVersion = new HashMap<>();

  private SolrZkClient client;

  public static final String zkQueryPath = "/monitor.json";

  public Monitor getMonitor() {
    return singletonMonitor;
  }

  /**
   * @param queryString The input query string
   * @param req         The corresponding reconstructed SolrQueryRequest (from params)
   * @return The parsed Lucene Query object
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
    if (singletonMonitor == null) {
      createMonitorInstance(req);
    }

    return new MonitorUpdateProcessor(req, rsp, next, getMonitor());
  }

  /**
   * Create a static monitor instance and initialize
   *
   * @param req The request which is used to get Zookeeper client
   */
  void createMonitorInstance(SolrQueryRequest req) {
    try {
      singletonMonitor = new Monitor(new StandardAnalyzer(), Presearcher.NO_FILTERING);
      ZkController zc = req.getCore().getCoreContainer().getZkController();
      this.client = zc.getZkClient();
      zc.addOnReconnectListener(new ZkReconnectListener(req));
      if (!client.exists(zkQueryPath, true)) {
        try {
          client.makePath(zkQueryPath, true);
        } catch (KeeperException e) {
          if (e.code() != KeeperException.Code.NODEEXISTS) {
            throw e;
          }
        }
      }
      syncQueries(req);
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }

  /**
   * Synchronize queries with Zookeeper.
   *
   * @param req The request is used to get the Solr Core to reconstruct the query request, which is used for parsing
   */
  synchronized void syncQueries(SolrQueryRequest req) {
    try {
      // if (client.exists(zkQueryPath, new MonitorWatcher(req), true) == null) return;
      byte[] bytesRead = client.getData(zkQueryPath, new MonitorWatcher(req), null, true);
      String jsonStr = "{}";
      if (bytesRead != null) {
        jsonStr = new String(bytesRead, StandardCharsets.UTF_8);
      }
      Map<String, Object> jsonMap = JsonUtil.parseJson(jsonStr);

      Monitor monitor = getMonitor();

      // find queries that need to be updated
      ArrayList<MonitorQuery> queriesToAdd = getNewQueries(req, jsonMap);
      List<String> queryIdsToDelete = new ArrayList<>();
      queriesToAdd.forEach(query -> queryIdsToDelete.add(query.getId()));

      // update queries
      monitor.deleteById(queryIdsToDelete);
      monitor.register(queriesToAdd);
    } catch (KeeperException | InterruptedException | JoseException | IOException ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }

  /**
   * Find the newly created or modified queries. They should be registered to the monitor.
   *
   * @param req        The request is used to get the Solr Core to reconstruct the query request, which is used for parsing
   * @param allQueries The map containing all queries, read from Zookeeper
   * @return A list of {@link MonitorQuery} that should be updated to local monitor
   * @throws IOException If an IO exception occurs
   */
  private ArrayList<MonitorQuery> getNewQueries(SolrQueryRequest req, Map<String, Object> allQueries) throws IOException {
    ArrayList<MonitorQuery> queries = new ArrayList<>();
    try {
      for (String queryId : allQueries.keySet()) {

        // read and deserialize values from Zk
        Object value = allQueries.get(queryId);
        Map<String, Object> queryNodeMap = (Map) value;
        // check if this query already exist in local monitor (with latest version)
        Long version = (Long) queryNodeMap.get(MonitorQueryRegisterHandler.ZK_KEY_VERSION);
        if (queryVersion.containsKey(queryId) && version.equals(queryVersion.get(queryId))) continue;
        queryVersion.put(queryId, version);

        String queryString = (String) queryNodeMap.get(MonitorQueryRegisterHandler.ZK_KEY_QUERY_STRING);
        String paramsStr = (String) queryNodeMap.get(MonitorQueryRegisterHandler.ZK_KEY_SOLR_PARAMS);

        // deserialize params
        byte[] paramBytes = Base64.base64ToByteArray(paramsStr);
        Map rawParamMap = (Map) getObject(paramBytes);
        Map<String, String[]> paramMap = new HashMap<>();


        for (Object rawKey : rawParamMap.keySet()) {
          String key = (String) rawKey;
          Object rawValue = rawParamMap.get(key);
          if (rawValue instanceof String[]) {
            paramMap.put(key, (String[]) rawValue);
          } else if (rawValue instanceof String) {
            paramMap.put(key, new String[]{(String) rawValue});
          } else {
            throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "The params of monitor query is neither Map or MultiMap of String.");
          }
        }

        SolrParams params = new MultiMapSolrParams(paramMap);

        // reconstruct SolrQueryRequest
        SolrQueryRequest reconstructedReq = new SolrQueryRequestBase(req.getCore(), params) {
          // default SolrQueryRequestBase
        };
        queries.add(new MonitorQuery(queryId, parse(queryString, reconstructedReq)));
      }
    } catch (
        Exception e) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, e);
    }
    return queries;
  }

  /**
   * Deserialize from a byte array using {@link JavaBinCodec}
   *
   * @param bytes Raw byte array
   * @return Deserialized object
   * @throws IOException If an IO exception occurs
   */
  private static Object getObject(byte[] bytes) throws IOException {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(new ByteArrayInputStream(bytes));
    }
  }
}
