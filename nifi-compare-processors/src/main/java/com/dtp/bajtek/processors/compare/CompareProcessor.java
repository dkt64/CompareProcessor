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
package com.dtp.bajtek.processors.compare;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import org.apache.commons.io.IOUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;

import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;

@Tags({ "json, math, compare, automation, calc, stream" })
@CapabilityDescription("Compare value of PLC signal in a stream with constant value")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class CompareProcessor extends AbstractProcessor {

    public static final PropertyDescriptor Signal_id = new PropertyDescriptor.Builder().name("signal_id")
            .displayName("Signal id descriptor").description("Signal field in json").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Signal_val = new PropertyDescriptor.Builder().name("signal_val")
            .displayName("Signal value descriptor").description("Value field in json").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Value = new PropertyDescriptor.Builder().name("val")
            .displayName("Value to compare with").description("Static value").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Operator = new PropertyDescriptor.Builder().name("op")
            .displayName("Operator").description("Comparation operator")
            .allowableValues("<", "==", ">", ">=", "!=", "<=") // <, ==, >, >=, !=, <=
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor OutputMessage = new PropertyDescriptor.Builder().name("output_message")
            .displayName("Output message").description("Output message").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();

    public static final PropertyDescriptor OutputMessage_id = new PropertyDescriptor.Builder().name("output_message_id")
            .displayName("Output message JSON descriptor").description("Output message JSON descriptor").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor OutputVal_id = new PropertyDescriptor.Builder().name("output_val_id")
            .displayName("Output value JSON descriptor").description("Output message JSON descriptor").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();

    public static final PropertyDescriptor RepeatDelay = new PropertyDescriptor.Builder().name("repeat_delay")
            .displayName("Delay for message repetition").description("Delay for message repetition").required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

    public static final Relationship MESSAGE = new Relationship.Builder().name("message")
            .description("New message sent if condition is met").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("Original flowfile").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public Boolean CheckValue(String op, BigDecimal val1, BigDecimal val2) {
        switch (op) {
        case "<":
            return val1.compareTo(val2) < 0 ? true : false;
        case "==":
            return val1.compareTo(val2) == 0 ? true : false;
        case ">":
            return val1.compareTo(val2) > 0 ? true : false;
        case ">=":
            return val1.compareTo(val2) >= 0 ? true : false;
        case "!=":
            return val1.compareTo(val2) != 0 ? true : false;
        case "<=":
            return val1.compareTo(val2) <= 0 ? true : false;
        }
        return false;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(Signal_id);
        descriptors.add(Signal_val);
        descriptors.add(Value);
        descriptors.add(Operator);
        descriptors.add(OutputMessage);
        descriptors.add(OutputMessage_id);
        descriptors.add(OutputVal_id);
        descriptors.add(RepeatDelay);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MESSAGE);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();
        if (flowfile == null) {
            return;
        }

        String prop_id_name = context.getProperty(Signal_id).evaluateAttributeExpressions(flowfile).getValue();
        String prop_id_val = context.getProperty(Signal_val).evaluateAttributeExpressions(flowfile).getValue();
        String prop_val = context.getProperty(Value).evaluateAttributeExpressions(flowfile).getValue();
        String prop_op = context.getProperty(Operator).getValue();
        String prop_message = context.getProperty(OutputMessage).evaluateAttributeExpressions(flowfile).getValue();
        String prop_outmessage_id = context.getProperty(OutputMessage_id).evaluateAttributeExpressions(flowfile)
                .getValue();
        String prop_outval_id = context.getProperty(OutputVal_id).evaluateAttributeExpressions(flowfile).getValue();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    String json = IOUtils.toString(in);

                    // Object obj = new JSONParser().parse(json);
                    JsonObject parsedJson = (JsonObject) Jsoner.deserialize(json);

                    String signal_name = (String) parsedJson.get(prop_id_name);
                    BigDecimal signal_val = (BigDecimal) parsedJson.get(prop_id_val);

                    BigDecimal val = new BigDecimal(prop_val);
                    if (CheckValue(prop_op, signal_val, val)) {
                        // 1 parametr
                        final JsonObject newJson = new JsonObject();
                        newJson.put(prop_outmessage_id, prop_message);
                        newJson.put(prop_outval_id, signal_name + " " + signal_val + prop_op + prop_val);
                        String outstring = newJson.toJson();
                        value.set(outstring);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string. " + ex.toString());
                }
            }
        });

        String results = value.get();
        if (results != null && !results.isEmpty()) {

            // // Write the results to an attribute
            // String results = value.get();
            // if (results != null && !results.isEmpty()) {
            // flowfile = session.putAttribute(flowfile, "match", results);
            // }

            // To write the results back out ot flow file
            flowfile = session.write(flowfile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(results.getBytes());
                }
            });

            session.transfer(flowfile, MESSAGE);
        } else {
            session.transfer(flowfile, FAILURE);
        }

    }
}
