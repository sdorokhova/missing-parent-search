/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.operate.search;

import io.camunda.operate.search.util.RetryOperation;
import jakarta.annotation.PreDestroy;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.concurrent.TimeUnit;

@Configuration
public class ElasticsearchConnector {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnector.class);

  @Autowired
  private ElasticsearchProperties operateProperties;

  private ElasticsearchClient elasticsearchClient;
  private boolean shutdown = false;

  @Bean
  public RestHighLevelClient esClient() {
    //some weird error when ELS sets available processors number for Netty - see https://discuss.elastic.co/t/elasticsearch-5-4-1-availableprocessors-is-already-set/88036/3
    System.setProperty("es.set.netty.runtime.available.processors", "false");
    return createEsClient(operateProperties);
  }

  public static void closeEsClient(RestHighLevelClient esClient) {
    if (esClient != null) {
      try {
        esClient.close();
      } catch (IOException e) {
        logger.error("Could not close esClient",e);
      }
    }
  }

  public RestHighLevelClient createEsClient(ElasticsearchProperties elsConfig) {
    logger.debug("Creating Elasticsearch connection...");
    final RestClientBuilder restClientBuilder =
        RestClient.builder(getHttpHost(elsConfig))
        .setHttpClientConfigCallback(httpClientBuilder -> configureHttpClient(httpClientBuilder, elsConfig));
    if (elsConfig.getConnectTimeout() != null || elsConfig.getSocketTimeout() != null) {
      restClientBuilder
          .setRequestConfigCallback(configCallback -> setTimeouts(configCallback, elsConfig));
    }
    final RestHighLevelClient esClient = new RestHighLevelClientBuilder(restClientBuilder.build())
        .setApiCompatibilityMode(true).build();
    if (!checkHealth(esClient, elsConfig)) {
      logger.warn("Elasticsearch cluster is not accessible");
    } else {
      logger.debug("Elasticsearch connection was successfully created.");
    }
    return esClient;
  }

  private HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder, ElasticsearchProperties elsConfig) {
    setupAuthentication(httpAsyncClientBuilder, elsConfig);
    if (elsConfig.getSsl() != null) {
      setupSSLContext(httpAsyncClientBuilder, elsConfig.getSsl());
    }
    return httpAsyncClientBuilder;
  }

  private void setupSSLContext(HttpAsyncClientBuilder httpAsyncClientBuilder, SslProperties sslConfig) {
    try {
      httpAsyncClientBuilder.setSSLContext(getSSLContext(sslConfig));
      if (!sslConfig.isVerifyHostname()) {
        httpAsyncClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
      }
    } catch (Exception e) {
      logger.error("Error in setting up SSLContext", e);
    }
  }

  private SSLContext getSSLContext(SslProperties sslConfig)
      throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    final KeyStore truststore = loadCustomTrustStore(sslConfig);
    final TrustStrategy trustStrategy = sslConfig.isSelfSigned() ? new TrustSelfSignedStrategy() : null; // default;
    if (truststore.size() > 0) {
      return SSLContexts.custom().loadTrustMaterial(truststore, trustStrategy).build();
    } else {
      // default if custom truststore is empty
      return SSLContext.getDefault();
    }
  }

  private KeyStore loadCustomTrustStore(SslProperties sslConfig) {
    try {
      final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null);
      // load custom es server certificate if configured
      final String serverCertificate = sslConfig.getCertificatePath();
      if (serverCertificate != null) {
        setCertificateInTrustStore(trustStore, serverCertificate);
      }
      return trustStore;
    } catch (Exception e) {
      String message = "Could not create certificate trustStore for the secured Elasticsearch Connection!";
      throw new RuntimeException(message, e);
    }
  }

  private void setCertificateInTrustStore(final KeyStore trustStore, final String serverCertificate) {
    try {
      Certificate cert = loadCertificateFromPath(serverCertificate);
      trustStore.setCertificateEntry("elasticsearch-host", cert);
    } catch (Exception e) {
      String message = "Could not load configured server certificate for the secured Elasticsearch Connection!";
      throw new RuntimeException(message, e);
    }
  }

  private Certificate loadCertificateFromPath(final String certificatePath)
      throws IOException, CertificateException {
    Certificate cert;
    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(certificatePath))) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");

      if (bis.available() > 0) {
        cert = cf.generateCertificate(bis);
        logger.debug("Found certificate: {}", cert);
      } else {
        throw new RuntimeException(
            "Could not load certificate from file, file is empty. File: " + certificatePath
        );
      }
    }
    return cert;
  }

  private Builder setTimeouts(
      final Builder builder,
      final ElasticsearchProperties elsConfig) {
    if (elsConfig.getSocketTimeout() != null) {
      builder.setSocketTimeout(elsConfig.getSocketTimeout());
    }
    if (elsConfig.getConnectTimeout() != null) {
      builder.setConnectTimeout(elsConfig.getConnectTimeout());
    }
    return builder;
  }

  private HttpHost getHttpHost(ElasticsearchProperties elsConfig) {
    try {
      final URI uri = new URI(elsConfig.getUrl());
      return new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Error in url: " + elsConfig.getUrl(), e);
    }
  }

  private void setupAuthentication(final HttpAsyncClientBuilder builder, ElasticsearchProperties elsConfig) {
    final String username = elsConfig.getUsername();
    final String password = elsConfig.getPassword();

    if (username == null || password == null || username.isEmpty() || password.isEmpty()) {
      logger.warn("Username and/or password for are empty. Basic authentication for elasticsearch is not used.");
      return;
    }
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY,
        new UsernamePasswordCredentials(username,password));
    builder.setDefaultCredentialsProvider(credentialsProvider);
  }

  public boolean checkHealth(RestHighLevelClient esClient, ElasticsearchProperties elsConfig) {
    try {
      return RetryOperation.<Boolean>newBuilder()
          .noOfRetry(10)
          .retryOn(IOException.class, ElasticsearchException.class)
          .retryPredicate(result -> !result && !shutdown)
          .delayInterval(3, TimeUnit.SECONDS)
          .message(String.format("Connect to Elasticsearch cluster [%s] at %s",
              elsConfig.getClusterName(),
              elsConfig.getUrl()))
          .retryConsumer(() -> {
            final ClusterHealthResponse clusterHealthResponse = esClient.cluster()
                .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
            return !clusterHealthResponse.getClusterName().isEmpty();
          })
          .build().retry();
    } catch (Exception e) {
      throw new RuntimeException("Couldn't connect to Elasticsearch. Abort.", e);
    }
  }

  @PreDestroy
  public void tearDown(){
    this.shutdown = true;
  }
}
