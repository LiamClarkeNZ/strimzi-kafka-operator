/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.strimzi.api.kafka.Crds;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.enterprise.context.ApplicationScoped;

/**
 * The entry-point to the topic operator.
 * Main responsibility is to deploy a {@link Session} with an appropriate Config and KubeClient,
 * redeploying if the config changes.
 */
@QuarkusMain
@ApplicationScoped
public class Main2 implements QuarkusApplication {

    private final static Logger LOGGER = LogManager.getLogger(Main2.class);
    private Vertx vertx;

    public static void main(String[] args) {
        LOGGER.info("TopicOperator {} is starting", Main2.class.getPackage().getImplementationVersion());
        Quarkus.run(Main2.class, args);
    }

    public Main2(Vertx vertx) {
        this.vertx = vertx;
    }


    public int run(String... args) {

        DefaultKubernetesClient kubeClient = new DefaultKubernetesClient();
        Crds.registerCustomKinds();
//        VertxOptions options = new VertxOptions().setMetricsOptions(
//                new MicrometerMetricsOptions()
//                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
//                        .setJvmMetricsEnabled(true)
////                        .setEnabled(true));
//        Session session = new Session(kubeClient, config);

        vertx.deployVerticle(Session.class.getName(), ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Session deployed");
            } else {
                LOGGER.error("Error deploying Session", ar.cause());
            }
        });

        Quarkus.waitForExit();
        return 0;
    }

}
