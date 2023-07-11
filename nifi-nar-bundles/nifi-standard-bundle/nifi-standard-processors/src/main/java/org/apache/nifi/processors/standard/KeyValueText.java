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
package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.krakens.grok.api.GrokUtils.getNameGroups;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"kv", "keyValue", "Text", "Regular Expression", "regex"})
@CapabilityDescription(
        "Evaluates one or more Regular Expressions against the content of a FlowFile.  "
                + "The results of those Regular Expressions are assigned to FlowFile Attributes.  "
                + "Regular Expressions are entered by adding user-defined properties; "
                + "the name of the property maps to the Attribute Name into which the result will be placed.  "
                + "The attributes are generated differently based on the enabling of named capture groups.  "
                + "If named capture groups are not enabled:  "
                + "The first capture group, if any found, will be placed into that attribute name."
                + "But all capture groups, including the matching string sequence itself will also be "
                + "provided at that attribute name with an index value provided, with the exception of a capturing group "
                + "that is optional and does not match - for example, given the attribute name \"regex\" and expression "
                + "\"abc(def)?(g)\" we would add an attribute \"regex.1\" with a value of \"def\" if the \"def\" matched. If "
                + "the \"def\" did not match, no attribute named \"regex.1\" would be added but an attribute named \"regex.2\" "
                + "with a value of \"g\" will be added regardless."
                + "If named capture groups are enabled:  "
                + "Each named capture group, if found will be placed into the attributes name with the name provided.  "
                + "If enabled the matching string sequence itself will be placed into the attribute name.  "
                + "If multiple matches are enabled, and index will be applied after the first set of matches. "
                + "The exception is a capturing group that is optional and does not match  "
                + "For example, given the attribute name \"regex\" and expression \"abc(?<NAMED>def)?(?<NAMED-TWO>g)\"  "
                + "we would add an attribute \"regex.NAMED\" with the value of \"def\" if the \"def\" matched.  We would  "
                + " add an attribute \"regex.NAMED-TWO\" with the value of \"g\" if the \"g\" matched regardless.  "
                + "The value of the property must be a valid Regular Expressions with one or more capturing groups. "
                + "If named capture groups are enabled, all capture groups must be named.  If they are not, then the  "
                + "processor configuration will fail validation.  "
                + "If the Regular Expression matches more than once, only the first match will be used unless the property "
                + "enabling repeating capture group is set to true. "
                + "If any provided Regular Expression matches, the FlowFile(s) will be routed to 'matched'. "
                + "If no provided Regular Expression matches, the FlowFile will be routed to 'unmatched' "
                + "and no attributes will be applied to the FlowFile.")
@DynamicProperty(name = "A FlowFile attribute", value = "A Regular Expression with one or more capturing group",
        description = "The first capture group, if any found, will be placed into that attribute name."
                + "But all capture groups, including the matching string sequence itself will also be "
                + "provided at that attribute name with an index value provided.")
public class KeyValueText extends AbstractProcessor {
    public static final String FILENAME_ATTRIBUTES = "flowfile.attributes";
    public static final String FILENAME_CONTENT = "flowfile.content";

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();
    public static final PropertyDescriptor PATTERN = new PropertyDescriptor.Builder()
            .name("pattern")
            .displayName("key与value匹配的正则表达式")
            .description("key与value匹配的正则表达式")
            .required(true)
            .addValidator(Validator.VALID)
            .defaultValue("(\\w+)=\"*((?<=\")[^\"]+(?=\")|([^\\s]+))\"*")
            .build();

    public static final PropertyDescriptor STORE = new PropertyDescriptor.Builder()
            .name("store")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .allowableValues(FILENAME_ATTRIBUTES,FILENAME_CONTENT)
            .defaultValue(FILENAME_CONTENT)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .autoTerminateDefault(true)
            .description("FlowFiles are routed to this relationship when the Regular Expression is successfully evaluated and the FlowFile is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .autoTerminateDefault(true)
            .description("FlowFiles are routed to this relationship when no provided Regular Expression matches the content of the FlowFile")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private  Pattern pattern ;
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_MATCH);
        rels.add(REL_NO_MATCH);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CHARACTER_SET);
        props.add(PATTERN);
        props.add(STORE);
        this.properties = Collections.unmodifiableList(props);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.createRegexValidator(0, 40, true))
                .required(false)
                .dynamic(false)
                .build();
    }

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        pattern = Pattern.compile(context.getProperty(PATTERN).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog logger = getLogger();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String contentString;
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });
        contentString = new String(buffer, 0, (int) flowFile.getSize(), charset);

        final Map<String, String> regexResults = new HashMap<>();
        final Matcher matcher = pattern.matcher(contentString);
        while (matcher.find()) {
            regexResults.put(matcher.group(1), matcher.group(2));
        }
        if (!regexResults.isEmpty()) {
            if (FILENAME_CONTENT.equals(context.getProperty(STORE).getValue())){
                flowFile = session.write(flowFile, out -> jsonObjectMapper.writer().writeValue(out,regexResults));
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
            }else {
                flowFile = session.putAllAttributes(flowFile, regexResults);
            }
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_MATCH);
            logger.info("Matched {} Regular Expressions and added attributes to FlowFile {}", new Object[]{regexResults.size(), flowFile});
        } else {
            session.transfer(flowFile, REL_NO_MATCH);
            logger.info("Did not match any Regular Expressions for  FlowFile {}", new Object[]{flowFile});
        }
    }
}
