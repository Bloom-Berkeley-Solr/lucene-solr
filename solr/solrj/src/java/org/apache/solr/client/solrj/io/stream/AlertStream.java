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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
import org.apache.solr.common.SolrException;


public class AlertStream extends TupleStream implements Expressible {
  CloseableHttpClient httpClient;
  TupleStream tupleStream;
  String targetURL;

  public AlertStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    String targetURL = null;

    // parameters by position
    TupleStream tupleStream = factory.constructStream(streamExpressions.get(0));
    StreamExpressionNamedParameter urlExpression = factory.getNamedOperand(expression, "url");

    if (urlExpression != null) {
      targetURL = ((StreamExpressionValue) urlExpression.getParameter()).getValue();
    }

    init(tupleStream, targetURL);
  }

  public AlertStream(TupleStream tupleStream, String targetURL) {
    init(tupleStream, targetURL);
  }

  public void init(TupleStream tupleStream, String targetURL) {
    this.tupleStream = tupleStream;
    this.targetURL = targetURL;
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
    httpClient = HttpClients.createDefault();
  }

  @Override
  public void close() throws IOException {
    tupleStream.close();
    httpClient.close();
  }

  @Override
  public Tuple read() throws IOException {
    Tuple tuple = tupleStream.read();

    if (!tuple.EOF) {
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

  void alert(Tuple tuple) throws IOException {
    try {
      Map<String, String> requestBody = new HashMap<String, String>();
      requestBody.put("matches", tuple.fields.toString());
      requestBody.put("requestTime", new Timestamp(System.currentTimeMillis()).toString());


      HttpPost httpPost= new HttpPost((URI.create(targetURL)));
      httpPost.setEntity(new StringEntity(requestBody.toString()));

      httpClient.execute(httpPost);
      //TODO: Error handling for non-successful response code
    } catch (ClientProtocolException cpe) {
      // Currently detecting authentication by string-matching the HTTP response
      // Perhaps SolrClient should have thrown an exception itself??
      if (cpe.getMessage().contains("HTTP ERROR 401") || cpe.getMessage().contentEquals("HTTP ERROR 403")) {
        int code = cpe.getMessage().contains("HTTP ERROR 401") ? 401 : 403;
        throw new SolrException(SolrException.ErrorCode.getErrorCode(code),
          "Solr requires authentication for " + targetURL + ". Please supply valid credentials. HTTP code=" + code);
      } else {
        throw new SolrException(SolrException.ErrorCode.UNKNOWN, cpe);
      }
    }
  }
}
