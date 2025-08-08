package com.github.joselalvarez.openehr.connect;

import com.github.joselalvarez.openehr.connect.source.exception.OpenEHRSourceConnectException;
import com.github.joselalvarez.openehr.connect.source.message.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

@Slf4j
public class JsonConverter implements Converter {

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public byte[] fromConnectData(String s, Schema schema, Object o) {

        try {
            if (CompositionEvent.supports(schema)) {
                return new CompositionEvent((Struct) o).toJsonByteArray();
            } else if (EhrStatusEvent.supports(schema)) {
                return new EhrStatusEvent((Struct) o).toJsonByteArray();
            }
        } catch (Exception e) {
            log.error("Serialization error: {}", e);
            throw OpenEHRSourceConnectException.serdeError(e);
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
