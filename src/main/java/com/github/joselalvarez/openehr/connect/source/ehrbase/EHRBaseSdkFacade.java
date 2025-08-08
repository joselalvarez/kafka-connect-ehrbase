package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ehrbase.openehr.dbformat.DbToRmFormat;
import org.ehrbase.openehr.sdk.serialisation.jsonencoding.CanonicalJson;

public class EHRBaseSdkFacade {

    public static ObjectMapper getCanonicalObjectMapper() {
        return CanonicalJson.MARSHAL_OM;
    }

    public static <R extends com.nedap.archie.rm.RMObject> R reconstructRmObject(Class<R> clazz, String dbJsonStr) {
        return DbToRmFormat.reconstructRmObject(clazz, dbJsonStr);
    }
}
