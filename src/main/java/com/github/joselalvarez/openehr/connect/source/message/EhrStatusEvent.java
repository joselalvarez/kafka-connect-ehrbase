package com.github.joselalvarez.openehr.connect.source.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class EhrStatusEvent {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String TYPE = "_type";
    public static final String TYPE_NAME = "EHR_STATUS_EVENT";

    public static final String EVENT_TYPE_FIELD = "event_type";
    public static final String TIME_COMMITTED_FIELD = "time_committed";

    public static final String EHR_ID_FIELD = "ehr_id";
    public static final String UID_FIELD = "uid";
    public static final String CONTRIBUTION_ID_FIELD = "contribution_id";
    public static final String VERSION_FIELD = "version";

    public static final String ARCHETYPE_ID_FIELD = "archetype_id";
    public static final String EHR_STATUS_ID_FIELD = "ehr_status_id";
    public static final String REPLACED_ID_FIELD = "replaced_id";

    public static final String SUBJECT_TYPE_FIELD = "subject_type";
    public static final String SUBJECT_NAMESPACE_FIELD = "subject_namespace";
    public static final String SUBJECT_ID_FIELD = "subject_id";
    public static final String SUBJECT_ID_SCHEME_FIELD = "subject_id_scheme";

    public static final String EHR_STATUS_FIELD = "ehr_status";

    public static final Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.struct()
                .name(EhrStatusEvent.class.getCanonicalName())
                .field(EVENT_TYPE_FIELD, Schema.STRING_SCHEMA)
                .field(TIME_COMMITTED_FIELD, Schema.STRING_SCHEMA)
                .field(EHR_ID_FIELD, Schema.STRING_SCHEMA)
                .field(UID_FIELD, Schema.STRING_SCHEMA)
                .field(CONTRIBUTION_ID_FIELD, Schema.STRING_SCHEMA)
                .field(VERSION_FIELD, Schema.INT32_SCHEMA)
                .field(ARCHETYPE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(EHR_STATUS_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(REPLACED_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_NAMESPACE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_ID_SCHEME_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(EHR_STATUS_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    public static boolean supports(Schema schema) {
        return schema != null && SCHEMA.name().equals(schema.name());
    }


    private Struct delegate;

    public EhrStatusEvent() {
        this.delegate = new Struct(SCHEMA);
    }

    public EhrStatusEvent(Struct delegate) {
        this.delegate = delegate;
    }

    public Struct getDelegate() {
        return delegate;
    }

    public byte [] toJsonByteArray() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put(EhrStatusEvent.TYPE, EhrStatusEvent.TYPE_NAME);
        map.put(EhrStatusEvent.EVENT_TYPE_FIELD, this.getEventType());
        map.put(EhrStatusEvent.TIME_COMMITTED_FIELD, this.getTimeCommitted());
        map.put(EhrStatusEvent.EHR_ID_FIELD, this.getEhrId());
        map.put(EhrStatusEvent.UID_FIELD, this.getUid());
        map.put(EhrStatusEvent.CONTRIBUTION_ID_FIELD, this.getContributionId());
        map.put(EhrStatusEvent.VERSION_FIELD, this.getVersion());
        map.put(EhrStatusEvent.ARCHETYPE_ID_FIELD, this.getArchetypeId());
        map.put(EhrStatusEvent.EHR_STATUS_ID_FIELD, this.getEhrStatusId());
        map.put(EhrStatusEvent.REPLACED_ID_FIELD, this.getReplacedId());

        map.put(EhrStatusEvent.SUBJECT_TYPE_FIELD, this.getSubjectType());
        map.put(EhrStatusEvent.SUBJECT_NAMESPACE_FIELD, this.getSubjectNamespace());
        map.put(EhrStatusEvent.SUBJECT_ID_FIELD, this.getSubjectId());
        map.put(EhrStatusEvent.SUBJECT_ID_SCHEME_FIELD, this.getSubjectIdScheme());

        if (this.getEhrStatus() != null && this.getEhrStatus().length > 0) {
            JsonNode ehrStatus = objectMapper.readValue(this.getEhrStatus(), JsonNode.class);
            map.put(EhrStatusEvent.EHR_STATUS_FIELD, ehrStatus);
        } else {
            map.put(EhrStatusEvent.EHR_STATUS_FIELD, null);
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

    public void setEhrStatusId(String ehrStatusId) {
        delegate.put(EHR_STATUS_ID_FIELD, ehrStatusId);
    }

    public String getEhrStatusId() {
        return delegate.getString(EHR_STATUS_ID_FIELD);
    }

    public void setReplacedId(String replacedId) {
        delegate.put(REPLACED_ID_FIELD, replacedId);
    }

    public String getReplacedId() {
        return delegate.getString(REPLACED_ID_FIELD);
    }

    public void setSubjectType(String subjectType) {
        delegate.put(SUBJECT_TYPE_FIELD, subjectType);
    }

    public String getSubjectType() {
        return delegate.getString(SUBJECT_TYPE_FIELD);
    }

    public void setSubjectNamespace(String namespace) {
        delegate.put(SUBJECT_NAMESPACE_FIELD, namespace);
    }

    public String getSubjectNamespace() {
        return delegate.getString(SUBJECT_NAMESPACE_FIELD);
    }

    public void setSubjectId(String subjectId) {
        delegate.put(SUBJECT_ID_FIELD, subjectId);
    }

    public String getSubjectId() {
        return delegate.getString(SUBJECT_ID_FIELD);
    }

    public void setSubjectIdScheme(String subjectIdScheme) {
        delegate.put(SUBJECT_ID_SCHEME_FIELD, subjectIdScheme);
    }

    public String getSubjectIdScheme() {
        return delegate.getString(SUBJECT_ID_SCHEME_FIELD);
    }

    public void setEhrStatus(byte[] composition) {
        delegate.put(EHR_STATUS_FIELD, composition);
    }

    public byte [] getEhrStatus() {
        return delegate.getBytes(EHR_STATUS_FIELD);
    }

}
