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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.monitor.MatchingQueries;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.MonitorQuerySerializer;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.jose4j.json.JsonUtil;

public class MonitorUpdateProcessor extends UpdateRequestProcessor {

  private final ZkController zkController;
  private final CloudDescriptor cloudDesc;
  private final String collection;
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;

  public MonitorUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    CoreContainer cc = req.getCore().getCoreContainer();
    zkController = cc.getZkController();
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    collection = cloudDesc.getCollectionName();
    this.req = req;
    this.rsp = rsp;
  }

  // TODO: what parse function should we use?
  static Query parse(String query) {
    try {
      StandardQueryParser parser = new StandardQueryParser();
      return parser.parse(query, "defaultField");
    } catch (QueryNodeException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    // TODO: how to handle exceptions
    try {
      // get queries from Zookeeper
      var client = zkController.getZkClient();
      String path = MonitorUpdateProcessorFactory.zkQueryPath;
      if (client.exists(path, true)) {
        // create monitor & register queries
        // TODO: use which analyzer and presearcher?
        Monitor monitor = new Monitor(new StandardAnalyzer(), Presearcher.NO_FILTERING);

        byte[] bytes = client.getData(path, null, null, true);
        if (bytes.length == 0) return;
        Map<String, Object> jsonMap = JsonUtil.parseJson(new String(bytes));

        for (String queryId : jsonMap.keySet()) {
          Object value = jsonMap.get(queryId);
          // TODO: which type of exception?
          if (!(value instanceof String))
            throw new Exception("value of json object must be a string (query)");
          String query = (String) value;
          monitor.register(new MonitorQuery(queryId, MonitorUpdateProcessorFactory.parse(query), query, new HashMap<String, String>()));
        }
        // BytesRef bytesRef = new BytesRef(bytes);
        // MonitorQuery monitorQuery = serializer.deserialize(bytesRef);


        // TODO: which MatcherFactory to use?
        // TODO: We must set forInPlaceUpdate=True to avoid copy fields, figure out why
        MatchingQueries<QueryMatch> matches = monitor.match(DocumentBuilder.toDocument(cmd.getSolrInputDocument(), req.getSchema()), QueryMatch.SIMPLE_MATCHER);
        List<String> matchedIds = new ArrayList<>();
        matches.getMatches().forEach(x -> matchedIds.add(x.getQueryId()));
        this.rsp.add("match_count", matches.getMatchCount());
        this.rsp.add("matches", matchedIds);
        super.processAdd(cmd);
      }
    } catch (/*KeeperException | InterruptedException | JoseException |*/ Exception ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }
}
