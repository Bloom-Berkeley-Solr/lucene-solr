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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
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

  private SolrZkClient client;

  public static Monitor getMonitor() {
    return singletonMonitor;
  }


  // TODO: which parser to use

  /**
   * parsing function used to parse queries read from zookeeper
   * Set configs to support {@link PointRangeQuery}: int, long, float, double
   *
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
      // TODO: clear and add is inefficient, consider use a smarter method
      monitor.clear();
      ArrayList<MonitorQuery> queries = new ArrayList<MonitorQuery>();

      for (String queryId : jsonMap.keySet()) {
        // read and deserialize values from Zk
        Object value = jsonMap.get(queryId);
        // TODO: maybe support other data types than String, e.g. boolean, int, float, ...
        if (!(value instanceof Map))
          throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Only support String as values in monitor queries json configuration");
        Map<String, Object> queryNodeMap = (Map) value;

        String queryString = (String) queryNodeMap.get(QueryRegisterHandler.ZK_KEY_QUERY_STRING);
        byte[] paramBytes = (byte[]) queryNodeMap.get(QueryRegisterHandler.ZK_KEY_SOLR_PARAMS);

        // deserialize params
        SolrParams params = SerializationUtils.deserialize(paramBytes);
        // String query = (String) value;

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
}
