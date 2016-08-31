package org.xbib.elasticsearch.jdbc.strategy.standard;


import org.xbib.elasticsearch.common.util.ControlKeys;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.helper.client.AcknowledgeMetric;
import org.xbib.elasticsearch.helper.client.DocumentIdentity;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class AcknowledgeTracker {
    private List<DocumentIdentityJob> jobs = new ArrayList<>();
    private AcknowledgeMetric acknowledgeMetric;

    public AcknowledgeTracker(AcknowledgeMetric acknowledgeMetric) {
        this.acknowledgeMetric = acknowledgeMetric;
    }

    public void trackJob(IndexableObject indexableObject, String optype) {
        // TODO: statische index en type dient uit feeder configuratie afkomstig te zijn of uit het indexable object
        DocumentIdentity documentIdentity = new DocumentIdentity("test","marcogerard",indexableObject.id(),optype);

        String job = indexableObject.meta(ControlKeys._job.name());

        jobs.add(new DocumentIdentityJob(documentIdentity,Long.valueOf(job)));
    }

    public List<Long> getFailedJobs() {
        return jobs.stream().filter(identityJob ->
                !acknowledgeMetric.getResult().containsKey(identityJob.getDocumentIdentity())
                || !acknowledgeMetric.getResult().get(identityJob.getDocumentIdentity()))
                .map(DocumentIdentityJob::getJob).collect(Collectors.toList());
    }

    public Long getMaxJob() {
        OptionalLong job = jobs.stream().map(DocumentIdentityJob::getJob).mapToLong(Long::longValue).max();
        if(job.isPresent()) {
            return job.getAsLong();
        }
        return 0L;
    }

    private class DocumentIdentityJob {
        private DocumentIdentity documentIdentity;
        private Long job;

        DocumentIdentityJob(DocumentIdentity documentIdentity, Long job) {
            this.documentIdentity = documentIdentity;
            this.job = job;
        }

        DocumentIdentity getDocumentIdentity() {
            return documentIdentity;
        }

        public Long getJob() {
            return job;
        }

        public void setJob(Long job) {
            this.job = job;
        }
    }

}
