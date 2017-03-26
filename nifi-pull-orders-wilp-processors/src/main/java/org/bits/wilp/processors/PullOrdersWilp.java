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
package org.bits.wilp.processors;

import com.google.gson.Gson;
import com.srinath.OrderService;
import com.srinath.OrderSimulator;
import com.srinath.VO.OrderProductsVO;
import com.srinath.mapping.OrderProducts;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"Wilp"})
@CapabilityDescription("Pulls the orders from shop")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class PullOrdersWilp extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("Order Start Id")
            .description("Order Start Id")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("On Success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private long previousOrderNumber = 0;


    private OrderSimulator orderSimulator;
    private OrderService orderService;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MY_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
        orderSimulator = new OrderSimulator();
        orderService = new OrderService();
    }

    @OnScheduled
    public void startOrderSimulator(final ProcessContext context) {

        orderSimulator.startSimulator();

    }

    @OnUnscheduled
    public void stopOrderSimulator(final ProcessContext context) {
        orderSimulator.stopSimulator();
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

        previousOrderNumber = context.getProperty(MY_PROPERTY).asInteger();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        try {

            List<OrderProductsVO> results = orderService.getOrdersAboveOrderNumber(previousOrderNumber);
            for (OrderProductsVO op : results) {

                previousOrderNumber = op.getOrder().getOrderId() + 1;

                getLogger().info("Previous Order Number : " + previousOrderNumber);

            }

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream outputStream) throws IOException {

                    Gson gson =new Gson();
                    String json = gson.toJson(results);

                    outputStream.write(json.getBytes());

                }
            });

            session.transfer(flowFile, MY_RELATIONSHIP);
            session.commit();
        } finally {
            // hibernateUtil.closeSessionFactory();
        }


    }
}
