package com.linkedin.venice.init;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;


public class ControllerClientBackedSystemSchemaInitializer implements SystemSchemaInitializer {
  private final ControllerClient controllerClient;
  private final AvroProtocolDefinition protocol;
  private final SchemaPresenceChecker schemaPresenceChecker;

  public ControllerClientBackedSystemSchemaInitializer(ControllerClient controllerClient, AvroProtocolDefinition protocol, SchemaPresenceChecker schemaPresenceChecker) {
    this.controllerClient = controllerClient;
    this.protocol = protocol;
    this.schemaPresenceChecker = schemaPresenceChecker;
  }

  @Override
  public void execute() {
    SchemaResponse response = controllerClient.retryableRequest(3, cc -> cc.addValueSchema(protocol.getSystemStoreName(),
            protocol.getCurrentProtocolVersionSchema().toString()));

    if (response.isError()) {
      throw new VeniceException("Failed to register value schema for " + protocol.getSystemStoreName() + ". Error: " + response.getError());
    }

    // Wait for newly registered schema to propagate to local fabric
    schemaPresenceChecker.verifySchemaVersionPresentOrExit();
  }
}
