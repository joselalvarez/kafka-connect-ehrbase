package com.github.joselalvarez.openehr.connect.source.service.model;

import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import lombok.Builder;
import lombok.Getter;

import java.time.ZonedDateTime;
import java.util.List;

@Getter
@Builder
public class CompositionChangeRequest {

    private ZonedDateTime fromDate;
    private ZonedDateTime toDate;
    private long maxPoll;

    private List<PartitionOffset> partitionOffsets;

    private String templateId;
    private String rootConcept;

}
