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
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


public class AlertStream extends TupleStream implements Expressible {

  TupleStream tupleStream;

  public AlertStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    // parameters by position
    TupleStream tupleStream = factory.constructStream(streamExpressions.get(0));

    init(tupleStream);
  }

  public void init(TupleStream tupleStream) {
    this.tupleStream = tupleStream;
  }

  public AlertStream(TupleStream tupleStream) {
    init(tupleStream);
  }

  @Override
  public void setStreamContext(StreamContext context) {
    tupleStream.setStreamContext(context);
  }

  @Override
  public List<TupleStream> children() {
    ArrayList<TupleStream> list = new ArrayList<>();
    list.add(tupleStream);
    return list;
  }

  @Override
  public void open() throws IOException {
    tupleStream.open();
  }

  @Override
  public void close() throws IOException {
    tupleStream.close();
  }

  @Override
  public Tuple read() throws IOException {
    Tuple tuple = tupleStream.read();

    if(!tuple.EOF) {
      alert(tuple);
    }
    return tuple;
  }

  @Override
  public StreamComparator getStreamSort() {
    return tupleStream.getStreamSort();
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(((Expressible) tupleStream).toExpression(factory));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
            tupleStream.toExplanation(factory)
        })
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory).toString());
  }

  void alert(Tuple tuple) {
    //TODO: Add alert actions
  }
}
