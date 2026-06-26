//Author: Gianluca Bianchi

BUILD_DIR 			= build
CLASSPATH 			= $(shell ./gradlew -q printClasspath)
CLASSES 			= $(BUILD_DIR)/java/kotlin/main
PL_MAIN				= smartfab.model.edge.ProductionLine
ADMIN_SERVER_MAIN 	= smartfab.SpringServer
ADMIN_CLIENT_MAIN	= smartfab.model.client.AdminClient

clean:
	./gradlew clean

build:
	./gradlew compileJava

run-mqtt-broker:
	docker compose up -d

run-pl-instance: build run-mqtt-broker
	java -cp "$(CLASSES):$(CLASSPATH)" $(PL_MAIN) $(ARGS)

run-admin-server: build run-mqtt-broker
	java -cp "$(CLASSES):$(CLASSPATH)" $(ADMIN_SERVER_MAIN) $(ARGS)

run-admin-client: build run-mqtt-broker
	java -cp "$(CLASSES):$(CLASSPATH)" $(ADMIN_CLIENT_MAIN) $(ARGS)
