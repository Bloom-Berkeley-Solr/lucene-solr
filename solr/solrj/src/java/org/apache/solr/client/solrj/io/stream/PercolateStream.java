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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

public class PercolateStream extends TupleStream implements Expressible {

  private DaemonStream daemonStream;
  String id = null;

  public PercolateStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // get parameters by index
    String destinationCollection = factory.getValueOperand(expression, 1);
    String topicCollection = factory.getValueOperand(expression, 2);
    verifyCollectionName(destinationCollection, expression);
    verifyCollectionName(topicCollection, expression);

    //zkHost
    String zkHost = findZkHost(factory, destinationCollection, expression);
    verifyZkHost(zkHost, destinationCollection, expression);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    StreamExpressionNamedParameter flParam = factory.getNamedOperand(expression, "fl");
    if(null == flParam) {
      throw new IOException("invalid TopicStream fl cannot be null");
    }

    StreamExpressionNamedParameter idExpression = factory.getNamedOperand(expression, ID);
    if(idExpression == null) {
      throw new IOException("Invalid expression id parameter expected");
    } else {
      id = ((StreamExpressionValue) idExpression.getParameter()).getValue();
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals(ID)) {
        solrParams.set(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    init(destinationCollection, topicCollection, id, zkHost, solrParams);
  }

  public PercolateStream(String destinationCollection, String topicCollection, String id, String zkHost, SolrParams solrParams) throws IOException{
    init(destinationCollection, topicCollection, id, zkHost, solrParams);

  }

  public void init(String destinationCollection, String topicCollection, String id, String zkHost, SolrParams solrParams) throws IOException{
    //TODO: whether or not add more parameters instead of default: e.g. runInterval, checkpointEvery, batchSize
    long initialCheckpoint = -1;
    long checkpointEvery = 2;
    int updateBatchSize = 100;
    long runInterval = 50;
    int queueSize = 0;

    String topicId = "percolate-".concat(UUID.randomUUID().toString());
    TopicStream topicStream = new TopicStream(zkHost, topicCollection, topicCollection, topicId,
                                              initialCheckpoint, checkpointEvery, solrParams);
    AlertStream alertStream = new AlertStream(topicStream);
    UpdateStream updateStream = new UpdateStream(destinationCollection, alertStream, zkHost, updateBatchSize);

    this.daemonStream = new DaemonStream(updateStream, id, runInterval, queueSize);
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
    //TODO: change return EOF to use queue as daemonStream
    HashMap m = new HashMap();
//    m.put("EOF", true);
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
