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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;

public class MonitorUpdateProcessor extends UpdateRequestProcessor {

  private final SolrQueryResponse rsp;
  private final Monitor monitor;

  public MonitorUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next, Monitor monitor) {
    super(next);
    this.rsp = rsp;
    this.monitor = monitor;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    MatchingQueries<QueryMatch> matches = monitor.match(cmd.getLuceneDocument(), QueryMatch.SIMPLE_MATCHER);
    List<String> matchedIds = new ArrayList<>();
    matches.getMatches().forEach(x -> matchedIds.add(x.getQueryId()));
    this.rsp.add("match_count", matches.getMatchCount());
    this.rsp.add("matches", matchedIds);
    super.processAdd(cmd);
  }
}
