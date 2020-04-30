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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import static org.apache.solr.common.params.CommonParams.ID;

public class MonitorStream extends TupleStream implements Expressible {

  private DaemonStream daemonStream;
  private final String UPDATE_OPERATOR = "update";
  private final String OPERATOR_PARAM = "operator";
  private final String RUN_INTERVAL_PARAM = "runInterval";
  private final String URL_PARAM = "url";
  private final String FIELD_PARAM = "fl";
  private final Set<String> NON_SOLR_PARAMS = Set.of(ID, OPERATOR_PARAM, RUN_INTERVAL_PARAM, URL_PARAM);

  public MonitorStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // get parameters by index
    String topicCollection = factory.getValueOperand(expression, 2);
    verifyCollectionName(topicCollection, expression);

    //zkHost
    String zkHost = findZkHost(factory, topicCollection, expression);
    verifyZkHost(zkHost, topicCollection, expression);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    StreamExpressionNamedParameter idExpression = factory.getNamedOperand(expression, ID);
    String id = null;
    if(idExpression == null) {
      throw new IOException("Invalid expression id parameter expected");
    } else {
      id = ((StreamExpressionValue) idExpression.getParameter()).getValue();
    }

    StreamExpressionNamedParameter runIntervalExpression = factory.getNamedOperand(expression, RUN_INTERVAL_PARAM);
    long runInterval;
    if(runIntervalExpression == null) {
      //default value for run_interval is set to 50
      runInterval = 50;
    } else {
      runInterval = Long.parseLong(((StreamExpressionValue) runIntervalExpression.getParameter()).getValue());
    }

    StreamExpressionNamedParameter operatorExpression = factory.getNamedOperand(expression, OPERATOR_PARAM);
    String operator = null;
    if(operatorExpression == null) {
      throw new IOException("Invalid expression for operator parameter");
    } else {
      operator = ((StreamExpressionValue) operatorExpression.getParameter()).getValue();
    }

    StreamExpressionNamedParameter flParam = factory.getNamedOperand(expression, FIELD_PARAM);
    if(null == flParam) {
      throw new IOException("invalid TopicStream fl cannot be null");
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      String paramName = namedParam.getName();
      if (!NON_SOLR_PARAMS.contains(paramName)) {
        solrParams.set(paramName, namedParam.getParameter().toString().trim());
      }
    }

    TopicStream topicStream = initTopicStream(topicCollection, zkHost, solrParams);
    if (operator.equals(UPDATE_OPERATOR)) {
      String destinationCollection = factory.getValueOperand(expression, 3);
      verifyCollectionName(destinationCollection, expression);
      init(id, topicStream, destinationCollection, zkHost, runInterval);

    } else {
      StreamExpressionNamedParameter urlExpression = factory.getNamedOperand(expression, URL_PARAM);
      String url = null;
      // Check url exist
      if(urlExpression == null) {
        throw new IOException("Invalid expression for url parameter ");
      } else {
        // Check url valid
        url = ((StreamExpressionValue) urlExpression.getParameter()).getValue();
        verifyUrl(url);
      }
      init(id, topicStream, url, runInterval);
    }
  }

  public MonitorStream(String id, String topicCollection, String destinationCollection, String zkHost,
                       SolrParams solrParams, long runInterval) throws IOException {
    TopicStream topicStream = initTopicStream(topicCollection, zkHost, solrParams);
    init(id, topicStream, destinationCollection, zkHost, runInterval);
  }

  public MonitorStream(String id, String topicCollection, String zkHost, SolrParams solrParams, String url,
                       long runInterval) throws IOException {
    TopicStream topicStream = initTopicStream(topicCollection, zkHost, solrParams);
    init(id, topicStream, url, runInterval);
  }

  public void init(String id, TopicStream topicStream, String url, long runInterval) throws IOException {
    AlertStream alertStream = new AlertStream(topicStream, url);
    this.daemonStream = new DaemonStream(alertStream, id, runInterval, 0);
  }

  public void init(String id, TopicStream topicStream, String destinationCollection, String zkHost, long runInterval) throws IOException{
    int updateBatchSize = 100;

    UpdateStream updateStream = new UpdateStream(destinationCollection, topicStream, zkHost, updateBatchSize);
    this.daemonStream = new DaemonStream(updateStream, id, runInterval, 0);
  }

  private TopicStream initTopicStream(String topicCollection, String zkHost, SolrParams solrParams) {
    long initialCheckpoint = -1;
    long checkpointEvery = -1;
    //TODO: Make checkpointEvery a configurable field and set default value to -1?

    String topicId = "monitor-".concat(UUID.randomUUID().toString());
    return new TopicStream(zkHost, topicCollection, topicCollection, topicId,
      initialCheckpoint, checkpointEvery, solrParams);
  }

  @Override
  public void setStreamContext(StreamContext context) {
    daemonStream.setStreamContext(context);
  }

  @Override
  public List<TupleStream> children() {
    ArrayList<TupleStream> list = new ArrayList<>();
    list.add(daemonStream);
    return list;
  }

  @Override
  public void open() throws IOException {
    daemonStream.open();
  }

  @Override
  public void close() throws IOException {
    daemonStream.close();
  }

  @Override
  public Tuple read() throws IOException {
    // Can't directly return daemonStream since queuesize for daemonStream is 0
    //TODO: change return EOF to use queue as daemonStream
    HashMap m = new HashMap();
    return new Tuple(m);
  }

  @Override
  public StreamComparator getStreamSort() {
    return daemonStream.getStreamSort();
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        daemonStream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory).toString());
  }

  public DaemonStream getDaemonStream() {
    return daemonStream;
  }

  public void shutdown() {
    daemonStream.shutdown();
  }

  private String findZkHost(StreamFactory factory, String collectionName, StreamExpression expression) {
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    if(null == zkHostExpression){
      String zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        return factory.getDefaultZkHost();
      } else {
        return zkHost;
      }
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      return ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }

    return null;
  }

  private void verifyUrl(String url) throws IOException{
    try {
      new URI(url).toURL();
    } catch (URISyntaxException x) {
      throw new IOException(x.getMessage(), x);
    }
  }

  private void verifyZkHost(String zkHost, String collectionName, StreamExpression expression) throws IOException {
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
  }

  private void verifyCollectionName(String collectionName, StreamExpression expression) throws IOException {
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }
  }
}
