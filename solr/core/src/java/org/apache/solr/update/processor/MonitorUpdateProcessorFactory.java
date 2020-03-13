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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class MonitorUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  // TODO: store all queries in one zkNode
  public static final String zkQueryPath = "/monitor.json";
  // public static final String zkParentPath = "/monitor";
  public static final String parserDefaultField = "defaultField";

  private static Monitor singletonMonitor = null;

  public static Monitor getMonitor() {
    if (singletonMonitor == null) {
      try{
        // TODO: use which analyzer and presearcher?
        singletonMonitor = new Monitor(new StandardAnalyzer(), Presearcher.NO_FILTERING);
      } catch (IOException ex) {
        throw new SolrException(SolrException.ErrorCode.UNKNOWN, ex);
      }
    }

    return singletonMonitor;
  }


  //TODO: which parser to use
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
    return new MonitorUpdateProcessor(req, rsp, next);
  }
}
