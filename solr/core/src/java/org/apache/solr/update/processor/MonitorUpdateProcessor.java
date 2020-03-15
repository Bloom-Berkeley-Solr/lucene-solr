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
import java.util.List;

import org.apache.lucene.monitor.MatchingQueries;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DocumentBuilder;

public class MonitorUpdateProcessor extends UpdateRequestProcessor {

  private final CloudDescriptor cloudDesc;
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;

  public MonitorUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    CoreContainer cc = req.getCore().getCoreContainer();
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    this.req = req;
    this.rsp = rsp;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    // TODO: how to handle exceptions
    try {
      // TODO: which MatcherFactory to use?
      // TODO: We must set forInPlaceUpdate=True to avoid copy fields, figure out why
      Monitor monitor = MonitorUpdateProcessorFactory.getMonitor();
      MatchingQueries<QueryMatch> matches = monitor.match(DocumentBuilder.toDocument(cmd.getSolrInputDocument(), req.getSchema()), QueryMatch.SIMPLE_MATCHER);
      List<String> matchedIds = new ArrayList<>();
      matches.getMatches().forEach(x -> matchedIds.add(x.getQueryId()));
      this.rsp.add("match_count", matches.getMatchCount());
      this.rsp.add("matches", matchedIds);
      super.processAdd(cmd);
    } catch (/*KeeperException | InterruptedException | JoseException |*/ Exception ex) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
    }
  }
}
