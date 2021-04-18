#!/bin/sh
mvn package -Djacoco.skip=true
mvn exec:java
