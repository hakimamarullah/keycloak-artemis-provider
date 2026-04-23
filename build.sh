#!/bin/bash

set -e

mvn clean package -DskipTests

rm -rf docker/providers/keycloak-artemis-provider*.jar

cp target/keycloak-artemis-provider*.jar docker/providers/