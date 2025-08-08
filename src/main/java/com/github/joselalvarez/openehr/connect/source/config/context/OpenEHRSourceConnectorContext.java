package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.message.CompositionEventMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusEventMapper;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;

import javax.sql.DataSource;

public interface OpenEHRSourceConnectorContext {
    void close();
    boolean isClosed();
    DataSource getEHRBaseDataSource();
    OpenEHRChangeLogService getOpenEHRChangeLogService();
    ObjectMapper getCanonicalObjectMapper();
    CompositionEventMapper getCompositionEventMapper();
    EhrStatusEventMapper getEhrStatusEventMapper();
    PartitionOffsetFactory getPartitionOffsetFactory();
}
