package org.xbib.elasticsearch.jdbc.strategy.standard;


import org.xbib.elasticsearch.helper.client.AcknowledgeMetric;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class AcknowledgeTracker {
    private List<Long> jobs = new ArrayList<>();
    private AcknowledgeMetric acknowledgeMetric;

    public AcknowledgeTracker(AcknowledgeMetric acknowledgeMetric) {
        this.acknowledgeMetric = acknowledgeMetric;
    }

    public void trackJob(Long jobId) {
        jobs.add(jobId);
    }

    public Long getHighestSucceededJobIdBeforeFirstFailedJob() {
        List<Long> failedJobs = getFailedJobs();
        LongStream jobStream = jobs.stream().mapToLong(Long::longValue);

        if (failedJobs.isEmpty()) {
            return jobStream.max().orElse(0L);
        } else {
            long lowestFailedJob = failedJobs.stream().mapToLong(Long::longValue).min().getAsLong();
            OptionalLong lowestSuccessFullJob = jobStream.filter(v -> v < lowestFailedJob).max();
            return lowestSuccessFullJob.orElse(0);
        }
    }

    List<Long> getFailedJobs() {
        return jobs.stream()
                .filter(identityJob -> !acknowledgeMetric.getResult().containsKey(identityJob)
                        || !acknowledgeMetric.getResult().get(identityJob))
                .collect(Collectors.toList());
    }
}
