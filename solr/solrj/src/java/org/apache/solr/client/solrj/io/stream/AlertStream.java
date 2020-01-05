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

// import javax.mail.Message;
// import javax.mail.MessagingException;
// import javax.mail.PasswordAuthentication;
// import javax.mail.Session;
// import javax.mail.Transport;
// import javax.mail.internet.InternetAddress;
// import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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


public class AlertStream extends TupleStream implements Expressible {

  TupleStream tupleStream;
  String emailRecipient;
  String emailTitle;

  public AlertStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    String emailRecipient = null;
    String emailTitle = null;

    // parameters by position
    TupleStream tupleStream = factory.constructStream(streamExpressions.get(0));
    StreamExpressionNamedParameter recipientExpression = factory.getNamedOperand(expression, "emailRecipient");
    StreamExpressionNamedParameter titleExpression = factory.getNamedOperand(expression, "emailTitle");

    if(recipientExpression != null) {
      emailRecipient= ((StreamExpressionValue) recipientExpression.getParameter()).getValue();
    }

    if(titleExpression != null) {
      emailTitle= ((StreamExpressionValue) titleExpression.getParameter()).getValue();
    }

    init(tupleStream, emailRecipient, emailTitle);
  }

  public void init(TupleStream tupleStream, String emailRecipient, String emailTitle) {
    this.tupleStream = tupleStream;
    this.emailRecipient = emailRecipient;
    this.emailTitle = emailTitle;
  }

  public AlertStream(TupleStream tupleStream, String emailRecipient, String emailTitle) {
    init(tupleStream, emailRecipient, emailTitle);
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
    return toExpression(factory, true);
  }

  public StreamExpressionParameter toExpression(StreamFactory factory, boolean includeStream) throws IOException {
    // TODO: add more parameters
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
        .withExpression(toExpression(factory, false).toString());
  }

  void alert(Tuple tuple) {
    if (emailRecipient != null && emailTitle != null) {
      emailAlert(tuple);
    }
  }

  void emailAlert(Tuple tuple){
//     final String username = "testalertstream@gmail.com";
//     final String password = "test1234!";
//
//     Properties prop = new Properties();
//     prop.put("mail.smtp.host", "smtp.gmail.com");
//     prop.put("mail.smtp.port", "587");
//     prop.put("mail.smtp.auth", "true");
//     prop.put("mail.smtp.starttls.enable", "true"); //TLS
//
//     Session session = Session.getInstance(prop,
//       new javax.mail.Authenticator() {
//         protected PasswordAuthentication getPasswordAuthentication() {
//           return new PasswordAuthentication(username, password);
//         }
//       });
//
//     try {
//       Message message = new MimeMessage(session);
//       message.setFrom(new InternetAddress(username));
//       message.setRecipients(
//         Message.RecipientType.TO,
//         //TODO: can change emailRecipient to list
//         InternetAddress.parse(this.emailRecipient)
//       );
//       message.setSubject(this.emailTitle);
//       message.setText("Hi Solr," + "\n\nThis is a test email for matching: " + tuple.fields.toString());
//
//       Transport.send(message);
//
//     } catch (MessagingException e) {
//       e.printStackTrace();
//     }
  }
}
