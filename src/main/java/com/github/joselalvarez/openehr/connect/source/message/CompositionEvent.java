package com.github.joselalvarez.openehr.connect.source.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class CompositionEvent {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String TYPE = "_type";
    public static final String TYPE_NAME = "COMPOSITION_EVENT";

    public static final String EVENT_TYPE_FIELD = "event_type";
    public static final String TIME_COMMITTED_FIELD = "time_committed";

    public static final String EHR_ID_FIELD = "ehr_id";
    public static final String UID_FIELD = "uid";
    public static final String CONTRIBUTION_ID_FIELD = "contribution_id";
    public static final String VERSION_FIELD = "version";

    public static final String ARCHETYPE_ID_FIELD = "archetype_id";
    public static final String TEMPLATE_ID_FIELD = "template_id";
    public static final String COMPOSITION_ID_FIELD = "composition_id";
    public static final String REPLACED_ID_FIELD = "replaced_id";
    public static final String COMPOSITION_FIELD = "composition";

    public static final Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.struct()
                        .name(CompositionEvent.class.getCanonicalName())
                        .field(EVENT_TYPE_FIELD, Schema.STRING_SCHEMA)
                        .field(TIME_COMMITTED_FIELD, Schema.STRING_SCHEMA)
                        .field(EHR_ID_FIELD, Schema.STRING_SCHEMA)
                        .field(UID_FIELD, Schema.STRING_SCHEMA)
                        .field(CONTRIBUTION_ID_FIELD, Schema.STRING_SCHEMA)
                        .field(VERSION_FIELD, Schema.INT32_SCHEMA)
                        .field(ARCHETYPE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(TEMPLATE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(COMPOSITION_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(REPLACED_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(COMPOSITION_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
                        .build();
    }

    public static boolean supports(Schema schema) {
        return schema != null && SCHEMA.name().equals(schema.name());
    }

    private Struct delegate;

    public CompositionEvent() {
        this.delegate = new Struct(SCHEMA);
    }

    public CompositionEvent(Struct delegate) {
        this.delegate = delegate;
    }

    public Struct getDelegate() {
        return delegate;
    }

    public byte [] toJsonByteArray() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put(CompositionEvent.TYPE, CompositionEvent.TYPE_NAME);
        map.put(CompositionEvent.EVENT_TYPE_FIELD, this.getEventType());
        map.put(CompositionEvent.TIME_COMMITTED_FIELD, this.getTimeCommitted());
        map.put(CompositionEvent.EHR_ID_FIELD, this.getEhrId());
        map.put(CompositionEvent.UID_FIELD, this.getUid());
        map.put(CompositionEvent.CONTRIBUTION_ID_FIELD, this.getContributionId());
        map.put(CompositionEvent.VERSION_FIELD, this.getVersion());
        map.put(CompositionEvent.ARCHETYPE_ID_FIELD, this.getArchetypeId());
        map.put(CompositionEvent.TEMPLATE_ID_FIELD, this.getTemplateId());
        map.put(CompositionEvent.COMPOSITION_ID_FIELD, this.getCompositionId());
        map.put(CompositionEvent.REPLACED_ID_FIELD, this.getReplacedId());

        if (this.getComposition() != null && this.getComposition().length > 0) {
            JsonNode composition = objectMapper.readValue(this.getComposition(), JsonNode.class);
            map.put(CompositionEvent.COMPOSITION_FIELD, composition);
        } else {
            map.put(CompositionEvent.COMPOSITION_FIELD, null);
        }

        return objectMapper.writeValueAsBytes(map);
    }

    public void setEventType(String eventType) {
        delegate.put(EVENT_TYPE_FIELD, eventType);
    }

    public String getEventType() {
        return delegate.getString(EVENT_TYPE_FIELD);
    }

    public void setTimeCommitted(String timeCommited) {
        delegate.put(TIME_COMMITTED_FIELD, timeCommited);
    }

    public String getTimeCommitted() {
        return delegate.getString(TIME_COMMITTED_FIELD);
    }

    public void setEhrId(String ehrId) {
        delegate.put(EHR_ID_FIELD, ehrId);
    }

    public String getEhrId() {
        return delegate.getString(EHR_ID_FIELD);
    }

    public void setUid(String ehrId) {
        delegate.put(UID_FIELD, ehrId);
    }

    public String getUid() {
        return delegate.getString(UID_FIELD);
    }

    public void setContributionId(String contributionId) {
        delegate.put(CONTRIBUTION_ID_FIELD, contributionId);
    }

    public String getContributionId() {
        return delegate.getString(CONTRIBUTION_ID_FIELD);
    }

    public void setVersion(Integer version) {
        delegate.put(VERSION_FIELD, version);
    }

    public Integer getVersion() {
        return delegate.getInt32(VERSION_FIELD);
    }

    public void setArchetypeId(String archetypeId) {
        delegate.put(ARCHETYPE_ID_FIELD, archetypeId);
    }

    public String getArchetypeId() {
        return delegate.getString(ARCHETYPE_ID_FIELD);
    }

    public void setTemplateId(String templateId) {
        delegate.put(TEMPLATE_ID_FIELD, templateId);
    }

    public String getTemplateId() {
        return delegate.getString(TEMPLATE_ID_FIELD);
    }

    public void setCompositionId(String compositionId) {
        delegate.put(COMPOSITION_ID_FIELD, compositionId);
    }

    public String getCompositionId() {
        return delegate.getString(COMPOSITION_ID_FIELD);
    }

    public void setReplacedId(String replacedId) {
        delegate.put(REPLACED_ID_FIELD, replacedId);
    }

    public String getReplacedId() {
        return delegate.getString(REPLACED_ID_FIELD);
    }

    public void setComposition(byte[] composition) {
        delegate.put(COMPOSITION_FIELD, composition);
    }

    public byte [] getComposition() {
        return delegate.getBytes(COMPOSITION_FIELD);
    }

}
