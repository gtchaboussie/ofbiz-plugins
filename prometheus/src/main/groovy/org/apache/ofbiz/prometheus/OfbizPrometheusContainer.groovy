package org.apache.ofbiz.prometheus

import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.core.metrics.Metric
import io.prometheus.metrics.exporter.httpserver.HTTPServer
import org.apache.ofbiz.base.container.Container
import org.apache.ofbiz.base.container.ContainerException
import org.apache.ofbiz.base.start.StartupCommand
import org.apache.ofbiz.base.util.Debug
import org.apache.ofbiz.base.util.UtilProperties
import org.apache.ofbiz.entity.Delegator
import org.apache.ofbiz.entity.DelegatorFactory
import org.apache.ofbiz.entity.GenericValue
import org.apache.ofbiz.entity.util.EntityQuery
import org.apache.ofbiz.service.LocalDispatcher
import org.apache.ofbiz.service.ServiceContainer

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class OfbizPrometheusContainer implements Container {

    private static final String MODULE = OfbizPrometheusContainer.getName()
    private static final String SERVICE_RUNNING_GAUGE = 'gauge-services-running'
    private static final String SERVICE_QUEUED_GAUGE = 'gauge-services-queued'
    private static final String SERVICE_PENDING_GAUGE = 'gauge-services-pending'

    private String name
    private boolean enabled
    String port
    HTTPServer myServer
    Delegator delegator
    LocalDispatcher dispatcher
    String instanceId
    String timeStep
    Map<String, Metric> myExpositions

    /**
     * returns the value of the activation property
     * @return the active value
     */
    static boolean getIsActive() {
        UtilProperties.getPropertyValue('prometheus.properties', 'prometheus.enable', 'N')
    }

    /**
     * returns the configured prometheus port
     * @return the port value
     */
    static String getPrometheusPort() {
        UtilProperties.getPropertyValue('prometheus.properties', 'prometheus.port', '9400')
    }

    /**
     * Returns the time step. It will be the interval used for running probes, in seconds.
     * @return the time step
     */
    static String getPrometheusTimeStep() {
        UtilProperties.getPropertyValue('prometheus.properties', 'prometheus.timestep', '60')
    }

    @Override
    void init(List<StartupCommand> ofbizCommands, String name, String configFile)
            throws ContainerException {
        this.name = name
        enabled = getIsActive()
        port = getPrometheusPort()
        delegator = DelegatorFactory.getDelegator('default')
        dispatcher = ServiceContainer.getLocalDispatcher('OfbizPrometheusContainer', delegator)
        timeStep = getPrometheusTimeStep()
        instanceId = UtilProperties.getPropertyValue('general', 'unique.instanceId')
        myExpositions = [:]
    }

    @Override
    boolean start() throws ContainerException {
        // TODO : setup the by instance startup condition
        if (!enabled) {
            Debug.logInfo('Not starting prometheus exposition port', MODULE)
            return false
        }
        if (!port) {
            Debug.logError("Can't start prometheus export without a set port", MODULE)
            return false
        }

        try {
            // start exsposure port
            int intPort = Integer.parseInt(port)
            myServer = HTTPServer.builder()
                    .port(intPort)
                    .buildAndStart()
            Debug.logInfo('Started prometheus exposition server', MODULE)

            // init metrics
            myExpositions.put(SERVICE_RUNNING_GAUGE,
                    Gauge.builder().name('services_running_on_instance')
                            .withExemplars()
                            .help('services currently running on instance')
                            .labelNames('instance_id', 'service_name', 'pool')
                            .register())
            myExpositions.put(SERVICE_QUEUED_GAUGE,
                    Gauge.builder()
                            .withExemplars()
                            .name('services_queued_on_instance')
                            .help('services currently queued on instance')
                            .labelNames('instance_id', 'service_name', 'pool')
                            .register())
            myExpositions.put(SERVICE_PENDING_GAUGE,
                    Gauge.builder()
                            .withExemplars()
                            .name('services_pending')
                            .help('services currently pending')
                            .labelNames('service_name', 'pool')
                            .register())

            // schedule
            ScheduledExecutorService runnerThread = Executors.newScheduledThreadPool(1)
            int timeStepInt = Integer.parseInt(timeStep)
            runnerThread.schedule(new PrometheusExposureRunner(delegator, dispatcher, timeStepInt), 30, TimeUnit.SECONDS)
        } catch (Exception e) {
            Debug.logError(e, 'Error while creating prometheur exposition server', MODULE)
            return false
        }
        Debug.logInfo('Prometheus local server started on port $port', MODULE)
        return true
    }

    @Override
    void stop()
            throws ContainerException {
        myServer.stop()
    }

    @Override
    String getName() {
        return name
    }

    /**
     * Actual runner that runs the probes
     */
    private class PrometheusExposureRunner implements Runnable {

        Delegator delegator
        LocalDispatcher dispatcher
        int timeStep

        PrometheusExposureRunner(Delegator delegator, LocalDispatcher dispatcher, int timeStep) {
            this.dispatcher = dispatcher
            this.delegator = delegator
            this.timeStep = timeStep
        }

        @Override
        void run() {
            try {
                while (true) {
                    runAllPrometheusProbes()
                    Debug.logInfo('Probes have been run y\'all', MODULE)
                    sleep(timeStep * 1000)
                }
            } catch (Exception e) {
                Debug.logError(e, 'Prometheus export failed with instanceId ' + instanceId, MODULE)
            }
        }

        void runAllPrometheusProbes() {
            List<GenericValue> servicesRunningOnInstance = EntityQuery.use(delegator).from('JobSandbox')
                    .where('statusId', 'SERVICE_RUNNING', 'runByInstanceId', instanceId)
                    .select('jobId', 'serviceName', 'poolId')
                    .queryList()
            List<GenericValue> servicesQueuedOnInstance = EntityQuery.use(delegator).from('JobSandbox')
                    .where('statusId', 'SERVICE_QUEUED', 'runByInstanceId', instanceId)
                    .select('jobId', 'serviceName', 'poolId')
                    .queryList()
            List<GenericValue> servicesPending = EntityQuery.use(delegator).from('JobSandbox')
                    .where('statusId', 'SERVICE_PENDING')
                    .select('jobId', 'serviceName', 'poolId')
                    .queryList()
            Map<List<String>, Object> servicesRunningAndCountByPool = getCountByServiceAndPool(servicesRunningOnInstance)
            Map<List<String>, Object> servicesQueuedAndCountByPool = getCountByServiceAndPool(servicesQueuedOnInstance)
            Map<List<String>, Object> servicesPendingAndCountByPool = getCountByServiceAndPool(servicesPending)

            if (servicesRunningAndCountByPool) {
                servicesRunningAndCountByPool.each { poolAndService, count ->
                    String service = poolAndService[0], pool = poolAndService[1]
                    myExpositions.get(SERVICE_RUNNING_GAUGE).labelValues(instanceId, service, pool).set(count)
                }
            } else {
                myExpositions.get(SERVICE_RUNNING_GAUGE).labelValues(instanceId, '', '').set(0)
            }

            if (servicesQueuedAndCountByPool) {
                servicesQueuedAndCountByPool.each { poolAndService, count ->
                    String service = poolAndService[0], pool = poolAndService[1]
                    myExpositions.get(SERVICE_QUEUED_GAUGE).labelValues(instanceId, service, pool).set(count)
                }
            } else {
                myExpositions.get(SERVICE_QUEUED_GAUGE).labelValues(instanceId, '', '').set(0)
            }

            if (servicesPendingAndCountByPool) {
                servicesPendingAndCountByPool.each { poolAndService, count ->
                    String service = poolAndService[0], pool = poolAndService[1]
                    myExpositions.get(SERVICE_PENDING_GAUGE).labelValues(service, pool).set(count)
                }
            } else {
                myExpositions.get(SERVICE_PENDING_GAUGE).labelValues('', '').set(0)
            }
        }

        Map<List<String>, Integer> getCountByServiceAndPool(List<Map> serviceList) {
            if (serviceList.isEmpty()) {
                return [:]
            }
            Map result = [:]
            serviceList.groupBy { [it.serviceName, it.poolId] }
                    .each { k, v -> result << [(k): v.size()] }
            return result
        }

    }

}
