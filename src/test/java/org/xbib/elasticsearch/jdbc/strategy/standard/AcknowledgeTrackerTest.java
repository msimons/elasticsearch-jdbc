package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.testng.annotations.Test;
import org.xbib.elasticsearch.helper.client.AcknowledgeMetric;

import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;


public class AcknowledgeTrackerTest {

    @Test
    public void test_GetFailedJobs_NoFailed() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> failedJobs = asList();

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getFailedJobs(), asList());
    }

    @Test
    public void test_GetFailedJobs_OneFailed() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,2L,3L,5L,6L);
        List<Long> failedJobs = asList(4L);

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getFailedJobs(), asList(4L));
    }

    @Test
    public void test_GetFailedJobs_OneAckMissing() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,2L,4L,5L,6L);
        List<Long> failedJobs = asList();

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getFailedJobs(), asList(3L));
    }

    @Test
    public void test_GetMaxJob_AllJobsSucceed() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> failedJobs = asList();

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getHighestSucceededJobIdBeforeFirstFailedJob(), (Long) 6L);
    }


    @Test
    public void test_GetMaxJob_OneJobFailed() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,2L,3L,4L,6L);
        List<Long> failedJobs = asList(5L);

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getHighestSucceededJobIdBeforeFirstFailedJob(), (Long) 4L);
    }


    @Test
    public void test_GetMaxJob_MultipleJobsFailed() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,3L,4L,6L);
        List<Long> failedJobs = asList(5L,2L);

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getHighestSucceededJobIdBeforeFirstFailedJob(), (Long) 1L);
    }

    @Test
    public void test_GetMaxJob_MultipleJobsFailedNotAllResultAck() {
        List<Long> jobs = asList(1L,2L,3L,4L,5L,6L);
        List<Long> succeededJobs = asList(1L,2L,6L);
        List<Long> failedJobs = asList(5L);

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getHighestSucceededJobIdBeforeFirstFailedJob(), (Long) 2L);
    }

    @Test
    public void test_GetMaxJob_NoJobsExecuted() {
        List<Long> jobs = asList();
        List<Long> succeededJobs = asList();
        List<Long> failedJobs = asList();

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getHighestSucceededJobIdBeforeFirstFailedJob(), (Long) 0L);
    }

    @Test
    public void test_GetMaxJob_AllJobsFailed() {
        List<Long> jobs = asList(2L,3L,4L);
        List<Long> succeededJobs = asList();
        List<Long> failedJobs = asList(2L,3L,4L);

        AcknowledgeTracker acknowledgeTracker = getAcknowledgeTracker(jobs, succeededJobs, failedJobs);

        assertEquals(acknowledgeTracker.getHighestSucceededJobIdBeforeFirstFailedJob(), (Long) 0L);
    }

    private AcknowledgeTracker getAcknowledgeTracker(List<Long> jobs, List<Long> succeeded, List<Long> failed) {
        AcknowledgeMetric acknowledgeMetric = new AcknowledgeMetric();
        AcknowledgeTracker acknowledgeTracker = new AcknowledgeTracker(acknowledgeMetric);

        jobs.forEach(j -> acknowledgeTracker.trackJob(j));
        succeeded.forEach(s -> acknowledgeMetric.addSucceeded(s));
        failed.forEach(s -> acknowledgeMetric.addFailed(s));

        return acknowledgeTracker;
    }

}