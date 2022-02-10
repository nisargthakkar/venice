package com.linkedin.venice;

import com.linkedin.venice.utils.Time;


public class ConfigConstants {
  // -------------------------- Common config default values --------------------------

  // Default D2 service name for local controller
  public static final String DEFAULT_VENICE_LOCAL_CONTROLLER_D2_SERVICE = "VeniceController";

  // Default D2 service name for parent controller
  public static final String DEFAULT_VENICE_PARENT_CONTROLLER_D2_SERVICE = "VeniceParentController";

  // ------------------------ Controller config default values ------------------------

  /**
   * Default value of sleep interval for polling topic deletion status from ZK.
   */
  public static final int DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS = 2 * Time.MS_PER_SECOND;

  public static final long DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS = 600;


  // -------------------------- Server config default values --------------------------

  /**
   * Default Kafka SSL context provider class name.
   *
   * {@link org.apache.kafka.common.security.ssl.BoringSslContextProvider} supports openssl.
   * BoringSSL is the c implementation of OpenSSL, and conscrypt add a java wrapper around BoringSSL.
   * The default BoringSslContextProvider mainly relies on conscrypt.
   */
  public static final String DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME = "org.apache.kafka.common.security.ssl.BoringSslContextProvider";
}
