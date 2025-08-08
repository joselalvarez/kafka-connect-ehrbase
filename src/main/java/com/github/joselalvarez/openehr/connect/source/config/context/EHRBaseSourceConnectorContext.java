package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseChangeLogService;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBasePartitionOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseRepository;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseSdkFacade;
import com.github.joselalvarez.openehr.connect.source.message.CompositionEventMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusEventMapper;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffsetFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import net.almson.object.ReferenceCountedObject;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;

@Slf4j
class EHRBaseSourceConnectorContext extends ReferenceCountedObject implements OpenEHRSourceConnectorContext {

    private boolean closed;

    private OpenEHRSourceConnectorConfig connectorConfig;
    private HikariDataSource hikariDataSource;
    private EHRBaseRepository ehrBaseRepository;
    private EHRBaseChangeLogService changeLogService;
    private ObjectMapper canonicalObjectMapper;
    private CompositionEventMapper compositionEventMapper;
    private EhrStatusEventMapper ehrStatusEventMapper;
    private EHRBasePartitionOffsetFactory partitionOffsetFactory;

    public EHRBaseSourceConnectorContext(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        // DataSource
        hikariDataSource = new HikariDataSource(new HikariConfig(connectorConfig.getJdbcProperties()));
        log.info("Connector[name={}]: EHRBase datasource created", connectorConfig.getConnectorName());
        // Beans
        partitionOffsetFactory = new EHRBasePartitionOffsetFactory(connectorConfig);
        ehrBaseRepository = new EHRBaseRepository(connectorConfig, new QueryRunner(hikariDataSource));
        changeLogService = new EHRBaseChangeLogService(ehrBaseRepository);
        canonicalObjectMapper = EHRBaseSdkFacade.getCanonicalObjectMapper();
        canonicalObjectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        compositionEventMapper = new CompositionEventMapper(canonicalObjectMapper, connectorConfig);
        ehrStatusEventMapper = new EhrStatusEventMapper(canonicalObjectMapper, connectorConfig);

    }

    @Override
    public DataSource getEHRBaseDataSource() {
        return hikariDataSource;
    }

    @Override
    public OpenEHRChangeLogService getOpenEHRChangeLogService() {
        return changeLogService;
    }

    @Override
    public ObjectMapper getCanonicalObjectMapper() {
        return canonicalObjectMapper;
    }

    @Override
    public CompositionEventMapper getCompositionEventMapper() {
        return compositionEventMapper;
    }

    @Override
    public EhrStatusEventMapper getEhrStatusEventMapper() {
        return ehrStatusEventMapper;
    }

    @Override
    public PartitionOffsetFactory getPartitionOffsetFactory() {
        return partitionOffsetFactory;
    }

    @Override
    protected void destroy() {
        closed = true;
        hikariDataSource.close();
        log.info("Connector[name={}]: EHRBase datasource closed", connectorConfig.getConnectorName());
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
