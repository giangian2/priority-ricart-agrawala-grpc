plugins {
    id("java")
    id("com.google.protobuf") version "0.10.0"
    id("org.springframework.boot") version "4.0.6"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "dps"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val grpcVersion = "1.81.0"
val protobufVersion = "4.34.1"

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Protocol Buffers
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")

    // gRPC
    runtimeOnly("io.grpc:grpc-netty-shaded:${grpcVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")

    // Spring library
    implementation("org.springframework.boot:spring-boot-starter-web")
    // For asynchronous Spring clients
    implementation("org.springframework.boot:spring-boot-starter-webflux")

    // MQTT
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }

    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }

    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

//try to run it directly without re-executing the gradle pipe
tasks.register<JavaExec>("runProductionLine") {
    group = "application"
    description = "runs a production line"    
    mainClass.set("smartfab.model.edge.ProductionLine")
    classpath = sourceSets["main"].runtimeClasspath
    
    if (project.hasProperty("args")) {
        val arguments = project.property("args") as String
        args(arguments.split(" "))
    }
}

//try to run it directly without re-executing the gradle pipe
tasks.register<JavaExec>("runAdminServer") {
    group = "application"
    description = "runs the spring admin server"    
    mainClass.set("smartfab.SpringServer")
    classpath = sourceSets["main"].runtimeClasspath
}

//ECTRACT DI RUNTIME EFFECTIVE CLASSPATH!!!!!
val printClasspath by tasks.registering {
    doLast {
        val classpath = sourceSets.main.get().runtimeClasspath.asPath
        println(classpath)
    }
}