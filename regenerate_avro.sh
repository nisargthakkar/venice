#!/bin/bash

mkdir -p avro
avro_tools=`ls avro | sort -rV | head -n 1`

if [ -z $avro_tools ]; then
  wget "https://artifactory.corp.linkedin.com:8083/artifactory/ext-libraries/org/apache/avro/avro-tools/1.4.0/avro-tools-1.4.0.jar" -P avro
  avro_tools=`ls avro | sort -rV | head -n 1`
fi

DEFAULT_AVRO_TOOLS_JAR="avro/$avro_tools"

AVRO_SCHEMAS_PATH=(
  "venice-common/src/main/resources/avro/KafkaMessageEnvelope/v6/*"
  "venice-common/src/main/resources/avro/PushJobStatusRecord/PushJobStatusRecordKey/v1/*"
  "venice-common/src/main/resources/avro/PushJobStatusRecord/PushJobStatusRecordValue/v1/*"
  "venice-common/src/main/resources/avro/PartitionState/v5/*"
  "venice-common/src/main/resources/avro/StoreVersionState/v3/*"
  "venice-common/src/main/resources/avro/ChunkedValueManifest/v-20/*"
  "venice-common/src/main/resources/avro/ChunkedKeySuffix/*"
  "venice-controller/src/main/resources/avro/AdminOperation/v20/*"
  "venice-schema-common/src/main/resources/avro/MultiGetResponseRecord/*"
  "venice-schema-common/src/main/resources/avro/MultiGetClientRequestKey/*"
  "venice-schema-common/src/main/resources/avro/MultiGetRouterRequestKey/*"
)
CODE_GEN_PATH=(
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-controller/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
)
FULL_CODE_GEN_PATH=(
  "${CODE_GEN_PATH[0]}/com/linkedin/venice/kafka/protocol/*.java"
  "${CODE_GEN_PATH[1]}/com/linkedin/venice/status/protocol/*.java"
  "${CODE_GEN_PATH[2]}/com/linkedin/venice/status/protocol/*.java"
  "${CODE_GEN_PATH[3]}/com/linkedin/venice/kafka/protocol/state/*.java"
  "${CODE_GEN_PATH[4]}/com/linkedin/venice/kafka/protocol/state/*.java"
  "${CODE_GEN_PATH[5]}/com/linkedin/venice/storage/protocol/*.java"
  "${CODE_GEN_PATH[6]}/com/linkedin/venice/storage/protocol/*.java"
  "${CODE_GEN_PATH[7]}/com/linkedin/venice/controller/kafka/protocol/admin/*.java"
  "${CODE_GEN_PATH[8]}/com/linkedin/venice/read/protocol/response/*.java"
  "${CODE_GEN_PATH[9]}/com/linkedin/venice/read/protocol/request/client/*.java"
  "${CODE_GEN_PATH[10]}/com/linkedin/venice/read/protocol/request/router/*.java"
)

if [[ $# < 1 ]]; then
  echo "Usage: $0 avro_tools_path"
  echo ""
  echo "    avro_tools_path: full path to the avro-tools-1.4.x.jar file (required). If you use 'default', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_JAR"
  echo ""
  echo "The $0 script uses avro-tools to generate SpecificRecord classes for the Avro schemas stored in:"
  echo ""
  for path in ${AVRO_SCHEMAS_PATH[@]}; do
      echo "    $path"
  done
  echo ""
  echo "The auto-generated classes are purged before each run and then re-generated here:"
  echo ""
  for path in ${FULL_CODE_GEN_PATH[@]}; do
      echo "    $path"
  done
  echo ""
  exit 1
fi

AVRO_TOOLS_PATH_PARAM=$1

if [[ $AVRO_TOOLS_PATH_PARAM -eq 'default' ]]; then
  AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_JAR
else
  AVRO_TOOLS_JAR=$AVRO_TOOLS_PATH_PARAM
fi

echo "Using AVRO_TOOLS_JAR=$AVRO_TOOLS_JAR"

for path in ${FULL_CODE_GEN_PATH[@]}; do
  rm $path
done

echo "Finished deleting old files. About to generate new ones..."

for (( i=0; i<${#FULL_CODE_GEN_PATH[@]}; i++ )); do
  java -jar $AVRO_TOOLS_JAR compile schema ${AVRO_SCHEMAS_PATH[i]} ${CODE_GEN_PATH[i]}
done

echo "Done!"
