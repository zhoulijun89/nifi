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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile")
})
@Tags({"lookup", "enrichment", "route", "record", "csv", "json", "avro", "database", "db", "logs", "convert", "filter"})
@CapabilityDescription("Extracts one or more fields from a Record and looks up a value for those fields in a LookupService. If a result is returned by the LookupService, "
    + "that result is optionally added to the Record. In this case, the processor functions as an Enrichment processor. Regardless, the Record is then "
    + "routed to either the 'matched' relationship or 'unmatched' relationship (if the 'Routing Strategy' property is configured to do so), "
    + "indicating whether or not a result was returned by the LookupService, allowing the processor to also function as a Routing processor. "
    + "The \"coordinates\" to use for looking up a value in the Lookup Service are defined by adding a user-defined property. Each property that is added will have an entry added "
    + "to a Map, where the name of the property becomes the Map Key and the value returned by the RecordPath becomes the value for that key. If multiple values are returned by the "
    + "RecordPath, then the Record will be routed to the 'unmatched' relationship (or 'success', depending on the 'Routing Strategy' property's configuration). "
    + "If one or more fields match the Result RecordPath, all fields "
    + "that match will be updated. If there is no match in the configured LookupService, then no fields will be updated. I.e., it will not overwrite an existing value in the Record "
    + "with a null value. Please note, however, that if the results returned by the LookupService are not accounted for in your schema (specifically, "
    + "the schema that is configured for your Record Writer) then the fields will not be written out to the FlowFile.")
@DynamicProperty(name = "Value To Lookup", value = "Valid Record Path", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                    description = "A RecordPath that points to the field whose value will be looked up in the configured Lookup Service")
@SeeAlso(value = {ConvertRecord.class, SplitRecord.class},
        classNames = {"org.apache.nifi.lookup.SimpleKeyValueLookupService", "org.apache.nifi.lookup.maxmind.IPLookupService", "org.apache.nifi.lookup.db.DatabaseRecordLookupService"})
public class LookupRecordSQL extends AbstractProcessor {

    private final RecordPathCache recordPathCache = new RecordPathCache(25);
    private volatile LookupService<?> lookupService;

    static final AllowableValue ROUTE_TO_SUCCESS = new AllowableValue("route-to-success", "Route to 'success'",
        "Records will be routed to a 'success' Relationship regardless of whether or not there is a match in the configured Lookup Service");
    static final AllowableValue ROUTE_TO_MATCHED_UNMATCHED = new AllowableValue("route-to-matched-unmatched", "Route to 'matched' or 'unmatched'",
        "Records will be routed to either a 'matched' or an 'unmatched' Relationship depending on whether or not there was a match in the configured Lookup Service. "
            + "A single input FlowFile may result in two different output FlowFiles.");

    static final AllowableValue RESULT_ENTIRE_RECORD = new AllowableValue("insert-entire-record", "Insert Entire Record",
        "The entire Record that is retrieved from the Lookup Service will be inserted into the destination path.");
    static final AllowableValue RESULT_RECORD_FIELDS = new AllowableValue("record-fields", "Insert Record Fields",
        "All of the fields in the Record that is retrieved from the Lookup Service will be inserted into the destination path.");

    static final AllowableValue USE_PROPERTY = new AllowableValue("use-property", "Use Property",
            "The \"Result RecordPath\" property will be used to determine which part of the record should be updated with the value returned by the Lookup Service");
    static final AllowableValue REPLACE_EXISTING_VALUES = new AllowableValue("replace-existing-values", "Replace Existing Values",
            "The \"Result RecordPath\" property will be ignored and the lookup service must be a single simple key lookup service. Every dynamic property value should "
            + "be a record path. For each dynamic property, the value contained in the field corresponding to the record path will be used as the key in the Lookup "
            + "Service and the value returned by the Lookup Service will be used to replace the existing value. It is possible to configure multiple dynamic properties "
            + "to replace multiple values in one execution. This strategy only supports simple types replacements (strings, integers, etc).");

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
        .name("lookup-service")
        .displayName("Lookup Service")
        .description("The Lookup Service to use in order to lookup a value in each Record")
        .identifiesControllerService(LookupService.class)
        .required(true)
        .build();

    static final PropertyDescriptor RESULT_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("result-record-path")
        .displayName("Result RecordPath")
        .description("A RecordPath that points to the field whose value should be updated with whatever value is returned from the Lookup Service. "
            + "If not specified, the value that is returned from the Lookup Service will be ignored, except for determining whether the FlowFile should "
            + "be routed to the 'matched' or 'unmatched' Relationship.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();

    static final PropertyDescriptor RESULT_CONTENTS = new PropertyDescriptor.Builder()
        .name("result-contents")
        .displayName("Record Result Contents")
        .description("When a result is obtained that contains a Record, this property determines whether the Record itself is inserted at the configured "
            + "path or if the contents of the Record (i.e., the sub-fields) will be inserted at the configured path.")
        .allowableValues(RESULT_ENTIRE_RECORD, RESULT_RECORD_FIELDS)
        .defaultValue(RESULT_ENTIRE_RECORD.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor ROUTING_STRATEGY = new PropertyDescriptor.Builder()
        .name("routing-strategy")
        .displayName("Routing Strategy")
        .description("Specifies how to route records after a Lookup has completed")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(ROUTE_TO_SUCCESS, ROUTE_TO_MATCHED_UNMATCHED)
        .defaultValue(ROUTE_TO_SUCCESS.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor REPLACEMENT_STRATEGY = new PropertyDescriptor.Builder()
        .name("record-update-strategy")
        .displayName("Record Update Strategy")
        .description("This property defines the strategy to use when updating the record with the value returned by the Lookup Service.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(REPLACE_EXISTING_VALUES, USE_PROPERTY)
        .defaultValue(USE_PROPERTY.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
        .name("record-path-lookup-miss-result-cache-size")
        .displayName("Cache Size")
        .description("Specifies how many lookup values/records should be cached."
                + "Setting this property to zero means no caching will be done and the table will be queried for each lookup value in each record. If the lookup "
                + "table changes often or the most recent data must be retrieved, do not use the cache.")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("0")
        .required(true)
        .build();

    static final Relationship REL_MATCHED = new Relationship.Builder()
        .name("matched")
        .description("All records for which the lookup returns a value will be routed to this relationship")
        .autoTerminateDefault(true)
        .build();
    static final Relationship REL_UNMATCHED = new Relationship.Builder()
        .name("unmatched")
        .description("All records for which the lookup does not have a matching value will be routed to this relationship")
        .autoTerminateDefault(true)
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All records will be sent to this Relationship if configured to do so, unless a failure occurs")
        .autoTerminateDefault(true)
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be enriched, the unchanged FlowFile will be routed to this relationship")
        .autoTerminateDefault(true)
        .build();

    private static final Set<Relationship> MATCHED_COLLECTION = Collections.singleton(REL_MATCHED);
    private static final Set<Relationship> UNMATCHED_COLLECTION = Collections.singleton(REL_UNMATCHED);
    private static final Set<Relationship> SUCCESS_COLLECTION = Collections.singleton(REL_SUCCESS);

    private volatile Set<Relationship> relationships = new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    private volatile boolean routeToMatchedUnmatched = false;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(LOOKUP_SERVICE);
        properties.add(RESULT_RECORD_PATH);
        properties.add(ROUTING_STRATEGY);
        properties.add(RESULT_CONTENTS);
        properties.add(REPLACEMENT_STRATEGY);
        properties.add(CACHE_SIZE);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("A RecordPath that points to the field whose value will be looked up in the configured Lookup Service")
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .dynamic(true)
            .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Set<String> dynamicPropNames = validationContext.getProperties().keySet().stream()
            .filter(PropertyDescriptor::isDynamic)
            .map(PropertyDescriptor::getName)
            .collect(Collectors.toSet());

        if (dynamicPropNames.isEmpty()) {
            return Collections.singleton(new ValidationResult.Builder()
                .subject("User-Defined Properties")
                .valid(false)
                .explanation("At least one user-defined property must be specified.")
                .build());
        }

        final Set<String> requiredKeys = validationContext.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class).getRequiredKeys();

        if(validationContext.getProperty(REPLACEMENT_STRATEGY).getValue().equals(REPLACE_EXISTING_VALUES.getValue())) {
            // it must be a single key lookup service
            if(requiredKeys.size() != 1) {
                return Collections.singleton(new ValidationResult.Builder()
                        .subject(LOOKUP_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("When using \"" + REPLACE_EXISTING_VALUES.getDisplayName() + "\" as Record Update Strategy, "
                                + "only a Lookup Service requiring a single key can be used.")
                        .build());
            }
        } else {
            final Set<String> missingKeys = requiredKeys.stream()
                .filter(key -> !dynamicPropNames.contains(key))
                .collect(Collectors.toSet());

            if (!missingKeys.isEmpty()) {
                final List<ValidationResult> validationResults = new ArrayList<>();
                for (final String missingKey : missingKeys) {
                    final ValidationResult result = new ValidationResult.Builder()
                        .subject(missingKey)
                        .valid(false)
                        .explanation("The configured Lookup Services requires that a key be provided with the name '" + missingKey
                            + "'. Please add a new property to this Processor with a name '" + missingKey
                            + "' and provide a RecordPath that can be used to retrieve the appropriate value.")
                        .build();
                    validationResults.add(result);
                }

                return validationResults;
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (ROUTING_STRATEGY.equals(descriptor)) {
            if (ROUTE_TO_MATCHED_UNMATCHED.getValue().equalsIgnoreCase(newValue)) {
                final Set<Relationship> matchedUnmatchedRels = new HashSet<>();
                matchedUnmatchedRels.add(REL_MATCHED);
                matchedUnmatchedRels.add(REL_UNMATCHED);
                matchedUnmatchedRels.add(REL_FAILURE);
                this.relationships = matchedUnmatchedRels;

                this.routeToMatchedUnmatched = true;
            } else {
                final Set<Relationship> successRels = new HashSet<>();
                successRels.add(REL_SUCCESS);
                successRels.add(REL_FAILURE);
                this.relationships = successRels;

                this.routeToMatchedUnmatched = false;
            }
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = original.getAttributes();

        final LookupContext lookupContext = createLookupContext(flowFile, context, session, writerFactory);
        final ReplacementStrategy replacementStrategy = createReplacementStrategy(context);

        final RecordSchema enrichedSchema;
        try {
            enrichedSchema = replacementStrategy.determineResultSchema(readerFactory, writerFactory, context, session, flowFile, lookupContext);
        } catch (final Exception e) {
            getLogger().error("Could not determine schema to use for enriched FlowFiles", e);
            session.transfer(original, REL_FAILURE);
            return;
        }

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        final Map<Relationship, RecordSchema> writeSchemas = new HashMap<>();

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            final Set<Relationship> relationships = replacementStrategy.lookup(record, context, lookupContext);

                            for (final Relationship relationship : relationships) {
                                // Determine the Write Schema to use for each relationship
                                RecordSchema writeSchema = writeSchemas.get(relationship);
                                if (writeSchema == null) {
                                    final RecordSchema outputSchema = enrichedSchema == null ? record.getSchema() : enrichedSchema;
                                    writeSchema = writerFactory.getSchema(originalAttributes, outputSchema);
                                    writeSchemas.put(relationship, writeSchema);
                                }

                                final RecordSetWriter writer = lookupContext.getRecordWriterForRelationship(relationship, writeSchema);
                                writer.write(record);
                            }
                        }
                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }
            });

            for (final Relationship relationship : lookupContext.getRelationshipsUsed()) {
                final RecordSetWriter writer = lookupContext.getExistingRecordWriterForRelationship(relationship);
                FlowFile childFlowFile = lookupContext.getFlowFileForRelationship(relationship);

                final WriteResult writeResult = writer.finishRecordSet();

                try {
                    writer.close();
                } catch (final IOException ioe) {
                    getLogger().warn("Failed to close Writer for {}", new Object[] {childFlowFile});
                }

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());

                childFlowFile = session.putAllAttributes(childFlowFile, attributes);
                session.transfer(childFlowFile, relationship);
                session.adjustCounter("Records Processed", writeResult.getRecordCount(), false);
                session.adjustCounter("Records Routed to " + relationship.getName(), writeResult.getRecordCount(), false);

                session.getProvenanceReporter().route(childFlowFile, relationship);
            }

        } catch (final Exception e) {
            getLogger().error("Failed to process {}", new Object[]{flowFile, e});

            for (final Relationship relationship : lookupContext.getRelationshipsUsed()) {
                final RecordSetWriter writer = lookupContext.getExistingRecordWriterForRelationship(relationship);
                final FlowFile childFlowFile = lookupContext.getFlowFileForRelationship(relationship);

                try {
                    writer.close();
                } catch (final Exception e1) {
                    getLogger().warn("Failed to close Writer for {}; some resources may not be cleaned up appropriately", writer);
                }

                session.remove(childFlowFile);
            }

            session.transfer(flowFile, REL_FAILURE);
            return;
        } finally {
            for (final Relationship relationship : lookupContext.getRelationshipsUsed()) {
                final RecordSetWriter writer = lookupContext.getExistingRecordWriterForRelationship(relationship);

                try {
                    writer.close();
                } catch (final Exception e) {
                    getLogger().warn("Failed to close Record Writer for {}; some resources may not be properly cleaned up", writer, e);
                }
            }
        }

        session.remove(flowFile);
        getLogger().info("Successfully processed {}, creating {} derivative FlowFiles and processing {} records",
            flowFile, lookupContext.getRelationshipsUsed().size(), replacementStrategy.getLookupCount());
    }

    private ReplacementStrategy createReplacementStrategy(final ProcessContext context) {
        final boolean isInPlaceReplacement = context.getProperty(REPLACEMENT_STRATEGY).getValue().equals(REPLACE_EXISTING_VALUES.getValue());

        if (isInPlaceReplacement) {
            return new InPlaceReplacementStrategy();
        } else {
            return new RecordPathReplacementStrategy(context);
        }
    }


    private class InPlaceReplacementStrategy implements ReplacementStrategy {
        private int lookupCount = 0;

        @Override
        public Set<Relationship> lookup(final Record record, final ProcessContext context, final LookupContext lookupContext) {
            lookupCount++;

            final Map<String, RecordPath> recordPaths = lookupContext.getRecordPathsByCoordinateKey();
            final Map<String, Object> lookupCoordinates = new HashMap<>(recordPaths.size());
            final String coordinateKey = lookupService.getRequiredKeys().iterator().next();
            final FlowFile flowFile = lookupContext.getOriginalFlowFile();

            boolean hasUnmatchedValue = false;
            for (final Map.Entry<String, RecordPath> entry : recordPaths.entrySet()) {
                final RecordPath recordPath = entry.getValue();

                final RecordPathResult pathResult = recordPath.evaluate(record);
                final List<FieldValue> lookupFieldValues = pathResult.getSelectedFields()
                    .filter(fieldVal -> fieldVal.getValue() != null)
                    .collect(Collectors.toList());

                if (lookupFieldValues.isEmpty()) {
                    final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                    getLogger().debug("RecordPath for property '{}' did not match any fields in a record for {}; routing record to {}", new Object[] {coordinateKey, flowFile, rels});
                    return rels;
                }

                for (final FieldValue fieldValue : lookupFieldValues) {
                    final Object coordinateValue = DataTypeUtils.convertType(fieldValue.getValue(), fieldValue.getField().getDataType(), null, null, null, fieldValue.getField().getFieldName());

                    lookupCoordinates.clear();
                    lookupCoordinates.put(coordinateKey, coordinateValue);

                    final Optional<?> lookupValueOption;
                    try {
                        lookupValueOption = lookupService.lookup(lookupCoordinates, flowFile.getAttributes());
                    } catch (final Exception e) {
                        throw new ProcessException("Failed to lookup coordinates " + lookupCoordinates + " in Lookup Service", e);
                    }

                    if (!lookupValueOption.isPresent()) {
                        hasUnmatchedValue = true;
                        continue;
                    }

                    final Object lookupValue = lookupValueOption.get();

                    final DataType inferredDataType = DataTypeUtils.inferDataType(lookupValue, RecordFieldType.STRING.getDataType());
                    fieldValue.updateValue(lookupValue, inferredDataType);
                }
            }

            if (hasUnmatchedValue) {
                return routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
            } else {
                return routeToMatchedUnmatched ? MATCHED_COLLECTION : SUCCESS_COLLECTION;
            }
        }

        @Override
        public RecordSchema determineResultSchema(final RecordReaderFactory readerFactory, final RecordSetWriterFactory writerFactory, final ProcessContext context, final ProcessSession session,
                                                  final FlowFile flowFile, final LookupContext lookupContext) throws IOException, SchemaNotFoundException, MalformedRecordException {

            try (final InputStream in = session.read(flowFile);
                 final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

                return reader.getSchema();
            }
        }

        @Override
        public int getLookupCount() {
            return lookupCount;
        }
    }


    private class RecordPathReplacementStrategy implements ReplacementStrategy {
        private int lookupCount = 0;

        private volatile Cache<Map<String, Object>, Optional<?>> cache;

        public RecordPathReplacementStrategy(ProcessContext context) {

            final int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();

            if (this.cache == null || cacheSize > 0) {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .build();
            }
        }

        @Override
        public Set<Relationship> lookup(final Record record, final ProcessContext context, final LookupContext lookupContext) {
            lookupCount++;

            final Map<String, Object> lookupCoordinates = createLookupCoordinates(record, lookupContext, true);
            if (lookupCoordinates.isEmpty()) {
                final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                return rels;
            }

            final FlowFile flowFile = lookupContext.getOriginalFlowFile();
            final Optional<?> lookupValueOption;
            final Optional<?> lookupValueCacheOption;

            try {
                lookupValueCacheOption = (Optional<?>) cache.get(lookupCoordinates, k -> null);
                if (lookupValueCacheOption == null) {
                    lookupValueOption = lookupService.lookup(lookupCoordinates, flowFile.getAttributes());
                } else {
                    lookupValueOption = lookupValueCacheOption;
                }
            } catch (final Exception e) {
                throw new ProcessException("Failed to lookup coordinates " + lookupCoordinates + " in Lookup Service", e);
            }

            if (!lookupValueOption.isPresent()) {
                final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                return rels;
            }

            applyLookupResult(record, context, lookupContext, lookupValueOption.get());

            final Set<Relationship> rels = routeToMatchedUnmatched ? MATCHED_COLLECTION : SUCCESS_COLLECTION;
            return rels;
        }

        private void applyLookupResult(final Record record, final ProcessContext context, final LookupContext lookupContext, final Object lookupValue) {
            // Ensure that the Record has the appropriate schema to account for the newly added values
            final RecordPath resultPath = lookupContext.getResultRecordPath();
            if (resultPath != null) {
                final RecordPathResult resultPathResult = resultPath.evaluate(record);

                final String resultContentsValue = context.getProperty(RESULT_CONTENTS).getValue();
                if (RESULT_RECORD_FIELDS.getValue().equals(resultContentsValue) && lookupValue instanceof Record) {
                    final Record lookupRecord = (Record) lookupValue;

                    // User wants to add all fields of the resultant Record to the specified Record Path.
                    // If the destination Record Path returns to us a Record, then we will add all field values of
                    // the Lookup Record to the destination Record. However, if the destination Record Path returns
                    // something other than a Record, then we can't add the fields to it. We can only replace it,
                    // because it doesn't make sense to add fields to anything but a Record.
                    resultPathResult.getSelectedFields().forEach(fieldVal -> {
                        final Object destinationValue = fieldVal.getValue();

                        if (destinationValue instanceof Record) {
                            final Record destinationRecord = (Record) destinationValue;

                            for (final String fieldName : lookupRecord.getRawFieldNames()) {
                                final Object value = lookupRecord.getValue(fieldName);

                                final Optional<RecordField> recordFieldOption = lookupRecord.getSchema().getField(fieldName);
                                if (recordFieldOption.isPresent()) {
                                    // Even if the looked up field is not nullable, if the lookup key didn't match with any record,
                                    // and matched/unmatched records are written to the same FlowFile routed to 'success' relationship,
                                    // then enriched fields should be nullable to support unmatched records whose enriched fields will be null.
                                    RecordField field = recordFieldOption.get();
                                    if (!routeToMatchedUnmatched && !field.isNullable()) {
                                        field = new RecordField(field.getFieldName(), field.getDataType(), field.getDefaultValue(), field.getAliases(), true);
                                    }
                                    destinationRecord.setValue(field, value);
                                } else {
                                    destinationRecord.setValue(fieldName, value);
                                }
                            }
                        } else {
                            final Optional<Record> parentOption = fieldVal.getParentRecord();
                            parentOption.ifPresent(parent -> parent.setValue(fieldVal.getField(), lookupRecord));
                        }
                    });
                } else {
                    final DataType inferredDataType = DataTypeUtils.inferDataType(lookupValue, RecordFieldType.STRING.getDataType());
                    resultPathResult.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(lookupValue, inferredDataType));
                }

                record.incorporateInactiveFields();
            }
        }

        @Override
        public RecordSchema determineResultSchema(final RecordReaderFactory readerFactory, final RecordSetWriterFactory writerFactory, final ProcessContext context, final ProcessSession session,
                                                  final FlowFile flowFile, final LookupContext lookupContext)
                throws IOException, SchemaNotFoundException, MalformedRecordException, LookupFailureException {

            final Map<String, String> flowFileAttributes = flowFile.getAttributes();
            try (final InputStream in = session.read(flowFile);
                 final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    final Map<String, Object> lookupCoordinates = createLookupCoordinates(record, lookupContext, false);
                    if (lookupCoordinates.isEmpty()) {
                        continue;
                    }

                    final Optional<?> lookupResult = lookupService.lookup(lookupCoordinates, flowFileAttributes);

                    cache.put(lookupCoordinates, lookupResult);

                    if (!lookupResult.isPresent()) {
                        continue;
                    }

                    applyLookupResult(record, context, lookupContext, lookupResult.get());
                    getLogger().debug("Found a Record for {} that returned a result from the LookupService. Will provide the following schema to the Writer: {}", flowFile, record.getSchema());
                    return record.getSchema();
                }

                getLogger().debug("Found no Record for {} that returned a result from the LookupService. Will provider Reader's schema to the Writer.", flowFile);
                return reader.getSchema();
            }
        }

        private Map<String, Object> createLookupCoordinates(final Record record, final LookupContext lookupContext, final boolean logIfNotMatched) {
            final Map<String, RecordPath> recordPaths = lookupContext.getRecordPathsByCoordinateKey();
            final Map<String, Object> lookupCoordinates = new HashMap<>(recordPaths.size());
            final FlowFile flowFile = lookupContext.getOriginalFlowFile();

            for (final Map.Entry<String, RecordPath> entry : recordPaths.entrySet()) {
                final String coordinateKey = entry.getKey();
                final RecordPath recordPath = entry.getValue();

                final RecordPathResult pathResult = recordPath.evaluate(record);
                final List<FieldValue> lookupFieldValues = pathResult.getSelectedFields()
                    .filter(fieldVal -> fieldVal.getValue() != null)
                    .collect(Collectors.toList());

                if (lookupFieldValues.isEmpty()) {
                    if (logIfNotMatched) {
                        final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                        getLogger().debug("RecordPath for property '{}' did not match any fields in a record for {}; routing record to {}", coordinateKey, flowFile, rels);
                    }

                    return Collections.emptyMap();
                }

                if (lookupFieldValues.size() > 1) {
                    if (logIfNotMatched) {
                        final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                        getLogger().debug("RecordPath for property '{}' matched {} fields in a record for {}; routing record to {}",
                            coordinateKey, lookupFieldValues.size(), flowFile, rels);
                    }

                    return Collections.emptyMap();
                }

                final FieldValue fieldValue = lookupFieldValues.get(0);
                final Object coordinateValue = DataTypeUtils.convertType(
                        fieldValue.getValue(),
                        Optional.ofNullable(fieldValue.getField())
                                .map(RecordField::getDataType)
                                .orElse(DataTypeUtils.inferDataType(fieldValue.getValue(), RecordFieldType.STRING.getDataType())),
                        null,
                        null,
                        null,
                        Optional.ofNullable(fieldValue.getField())
                                .map(RecordField::getFieldName)
                                .orElse(coordinateKey)
                );
                lookupCoordinates.put(coordinateKey, coordinateValue);
            }

            return lookupCoordinates;
        }

        @Override
        public int getLookupCount() {
            return lookupCount;
        }
    }


    protected LookupContext createLookupContext(final FlowFile flowFile, final ProcessContext context, final ProcessSession session, final RecordSetWriterFactory writerFactory) {
        final Map<String, RecordPath> recordPaths = new HashMap<>();
        for (final PropertyDescriptor prop : context.getProperties().keySet()) {
            if (!prop.isDynamic()) {
                continue;
            }

            final String pathText = context.getProperty(prop).evaluateAttributeExpressions(flowFile).getValue();
            final RecordPath lookupRecordPath = recordPathCache.getCompiled(pathText);
            recordPaths.put(prop.getName(), lookupRecordPath);
        }

        final RecordPath resultRecordPath;
        if (context.getProperty(RESULT_RECORD_PATH).isSet()) {
            final String resultPathText = context.getProperty(RESULT_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
            resultRecordPath = recordPathCache.getCompiled(resultPathText);
        } else {
            resultRecordPath = null;
        }

        return new LookupContext(recordPaths, resultRecordPath, session, flowFile, writerFactory, getLogger());
    }

    private interface ReplacementStrategy {
        Set<Relationship> lookup(Record record, ProcessContext context, LookupContext lookupContext);

        RecordSchema determineResultSchema(RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory, ProcessContext context, ProcessSession session, FlowFile flowFile,
                                           LookupContext lookupContext) throws IOException, SchemaNotFoundException, MalformedRecordException, LookupFailureException;

        int getLookupCount();
    }


    private static class LookupContext {
        private final Map<String, RecordPath> recordPathsByCoordinateKey;
        private final RecordPath resultRecordPath;
        private final ProcessSession session;
        private final FlowFile flowFile;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;

        private final Map<Relationship, Tuple<FlowFile, RecordSetWriter>> writersByRelationship = new HashMap<>();


        public LookupContext(final Map<String, RecordPath> recordPathsByCoordinateKey, final RecordPath resultRecordPath, final ProcessSession session, final FlowFile flowFile,
                             final RecordSetWriterFactory writerFactory, final ComponentLog logger) {
            this.recordPathsByCoordinateKey = recordPathsByCoordinateKey;
            this.resultRecordPath = resultRecordPath;
            this.session = session;
            this.flowFile = flowFile;
            this.writerFactory = writerFactory;
            this.logger = logger;
        }

        public Map<String, RecordPath> getRecordPathsByCoordinateKey() {
            return recordPathsByCoordinateKey;
        }

        public RecordPath getResultRecordPath() {
            return resultRecordPath;
        }

        public FlowFile getOriginalFlowFile() {
            return flowFile;
        }

        private Set<Relationship> getRelationshipsUsed() {
            return writersByRelationship.keySet();
        }

        public FlowFile getFlowFileForRelationship(final Relationship relationship) {
            final Tuple<FlowFile, RecordSetWriter> tuple = writersByRelationship.get(relationship);
            return tuple.getKey();
        }

        public RecordSetWriter getExistingRecordWriterForRelationship(final Relationship relationship) {
            final Tuple<FlowFile, RecordSetWriter> tuple = writersByRelationship.get(relationship);
            return tuple.getValue();
        }

        public RecordSetWriter getRecordWriterForRelationship(final Relationship relationship, final RecordSchema schema) throws IOException, SchemaNotFoundException {
            final Tuple<FlowFile, RecordSetWriter> tuple = writersByRelationship.get(relationship);
            if (tuple != null) {
                return tuple.getValue();
            }

            final FlowFile outFlowFile = session.create(flowFile);
            final OutputStream out = session.write(outFlowFile);
            try {
                final RecordSchema recordWriteSchema = writerFactory.getSchema(flowFile.getAttributes(), schema);
                final RecordSetWriter recordSetWriter = writerFactory.createWriter(logger, recordWriteSchema, out, outFlowFile);
                recordSetWriter.beginRecordSet();

                writersByRelationship.put(relationship, new Tuple<>(outFlowFile, recordSetWriter));
                return recordSetWriter;
            } catch (final Exception e) {
                try {
                    out.close();
                } catch (final Exception e1) {
                    e.addSuppressed(e1);
                }

                throw e;
            }
        }
    }
}
