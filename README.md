server.max-http-request-header-size=100KB
spring.main.banner-mode=off
server.compression.enabled=true
server.port=10080
mobile-proxy.whitelist-file=mobile-proxy-whitelist.restricted.properties
spring.config.import=actuator.properties,${mobile-proxy.whitelist-file}
springdoc.api-docs.path=/api-docs
springdoc.writer-with-order-by-keys=true
springdoc.writer-with-default-pretty-printer=true
springdoc.override-with-generic-response=false
springdoc.remove-broken-reference-definitions=false

# DB
spring.liquibase.change-log=classpath:db/changelog/changelog-master.xml
spring.jpa.hibernate.naming.physical-strategy=org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy
spring.jpa.hibernate.naming.implicit-strategy=org.springframework.boot.orm.jpa.hibernate.SpringImplicitNamingStrategy
# Connection pool
spring.datasource.hikari.minimum-idle=2
spring.datasource.hikari.maximum-pool-size=10

# Service config
# Will try to automatically connect to the IoTHub on startup
app.auto-connect=true
# Max time before a message is considered to be permanently failed
app.max-retry-age=24h
# Time between tries to resend failed messages
app.retry-failed.fixed-rate-minutes=1
# Initial delay on startup, before first try to resend failed messages
app.retry-failed.initial-delay-minutes=1
app.max-pending-messages=10000
app.max-failed-messages-per-try=${rate-limit.message.max-capacity}
app.failed-messages-page-size=100
app.default-priority=999
# Basic Auth (PW = xRH6QUdh0J-Bs640U3Za)
app.basic-auth.enabled=false
app.basic-auth.username=messagingUser
app.basic-auth.password={bcrypt}$2a$10$wwGCTxq66b6lUsuYpT31fOKJiEYg9SLhEYValHzhFe6tfY/pJkWzi
# DairyNet communication config
dairynet.host=http://backend:8080
dairynet.basic-auth.username=
dairynet.basic-auth.password=
dairynet.backup.username=farmview
dairynet.backup.password=farmview
dairynet.version-endpoint=/version

dairynet.velos.endpoint=/farmview/velosrouting?action=versioninformation
dairynet.velos.username=farmview
dairynet.velos.password=farmview

# Cloud Connector Connection Check Configuration
cloud-connection-check.username=farmview
cloud-connection-check.password=farmview
# Cloud Connector Connection Check Configuration Defaults
#cloud-connection-check.type-id-connection-lost=12002
#cloud-connection-check.type-id-connection-restored=12003
#cloud-connection-check.category=CLOUD_CONNECTOR
#cloud-connection-check.sender-domain=Cloud Connector
#cloud-connection-check.sender-node=Cloud Connector
#cloud-connection-check.sender-type=technical
#cloud-connection-check.check-interval=PT5M

dairynet.not-reachable.notification-id=12004
dairynet.not-reachable.severity=3
# Backup Scan Config
backup.enabled=false
backup.directory=./backups
backup.scan-delay=10000
# Ant-Style file filter for the name of backup files
# Only active when DairyNet Version is >= 44
backup.file-filter=*.zip
# If true will compress the backup files additionally with the specified compression-type
# Only active when DairyNet Version is >= 44
backup.compress-backups=false
# Either gz or zip
backup.compression-type=gz
# If true, will prefix the uploaded file with a timestamp of the files last modification
backup.prefix-with-timestamp=true
backup.creation-endpoint=/farmview/systemAdministration/requestBackup
# Backup Edge-Storage configuration
backup.storage.container=backup
# If Edge-Storage should be used, or the file uploaded by requesting a SAS token
backup.storage.use-edge-storage=false

# AFS communication config
afs.host=http://farmviewconnector:9002/v1.0/remote
afs.default-active-time-in-seconds=900


#Do not change this value in this file: Could expose PII
#Enables request body logging for Api error logs
log.api-error.request-body=false
# Loggers (classes or packages) and exception-classes to downgrade from ERROR to WARN during runtime
log.runtime.downgrade-loggers=\
  com.azure.core.amqp.implementation.RetryUtil,\
  com.azure.core.http.netty.NettyAsyncHttpClient,\
  com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient,\
  io.r2dbc.postgresql.client.ReactorNettyClient
log.runtime.downgrade-exceptions=
# Loggers (classes or packages) and exception-classes to downgrade from ERROR to WARN during shutdown
log.shutdown.downgrade-loggers=\
  com.azure.core.amqp.implementation.RetryUtil,\
  com.azure.identity.implementation.IdentityClient,\
  com.azure.identity.implementation.PowershellManager
log.shutdown.downgrade-exceptions=\
  java.util.concurrent.RejectedExecutionException,\
  org.springframework.core.task.TaskRejectedException,\
  reactor.netty.http.client.PrematureCloseException

mqtt.enabled=true
mqtt.host=host.docker.internal
mqtt.port=1883
mqtt.subscribed-topics=gea/par/+/sysSt,gea/sys/farm/ct/cfg,gea/app/dnc/#

dataexplorer.retrieval-endpoint=/dairynet-cloud/analytics/cms
dataexplorer.username=dairynetcloud
dataexplorer.password=DAIRYNETCLOUD
dataexplorer.transmit-as-avro=true

# Rate limiting for MQTT messages
# The topics to buffer messages for e.g. gea/par/bufferMe1/sysSt,gea/par/bufferMe2/sysSt
rate-limit.mqtt.buffer.topics=
rate-limit.mqtt.buffer.size=1000
rate-limit.mqtt.refill-interval=10s
rate-limit.mqtt.refill-tokens=10
rate-limit.mqtt.max-capacity=60

# Rate limiting for general messages
rate-limit.message.refill-interval=10s
rate-limit.message.refill-tokens=10
rate-limit.message.max-capacity=60

# These variables should be set automatically by the IoT Edge runtime when running the container on an edge device
iot-edge.device-id=
iot-edge.iot-hub-host-name=
iot-edge.module-id=
iot-edge.workload-uri=

directmethod-lib.use-managed-identity=false
directmethod-lib.use-on-iot-device=true
directmethod-lib.max-payload-size=100000

edgehub.metrics-url=http://edgehub:9600/metrics
edgehub.check-interval=30s

failed-messages-cleanup.cron-expression=0 0 0 * * *
dairynet-data-retrieval.cron-expression=0 59 * * * *
dairy-net-version-check.cron-expression=0 0 0 * * *
dairy-net-velos-version.cron-expression=0 0 0 * * *
token-information-cleanup.cron-expression=0 0 0 * * *

event-trail.cleanup.max-age=P365D

user-information.cache-time=P1D
farm-site-connector-service.endpoint=https://dairynet.dev.gea.com
remote-access-service.endpoint=${farm-site-connector-service.endpoint}

jwt.issuer-uri=https://login.portal.tst.gea.com/f5e22871-4b99-495e-affa-77a8b33a6686/v2.0/
jwt.jwk-set-uri=https://login.portal.tst.gea.com/geaidtst.onmicrosoft.com/b2c_1_signin_signup/discovery/v2.0/keys
spring.security.oauth2.resourceserver.jwt.issuer-uri=${jwt.issuer-uri}
spring.security.oauth2.resourceserver.jwt.jwk-set-uri=${jwt.jwk-set-uri}

# RabbitMQ Shovel config
rabbitmq.shovel-config.enabled=false
rabbitmq.shovel-config.verify-peer=true
rabbitmq.shovel-config.dairynet.host=http://host.docker.internal:15672
rabbitmq.shovel-config.dairynet.username=dairynet
rabbitmq.shovel-config.dairynet.password=dairynet
rabbitmq.shovel-config.dairynet.vhost=/
rabbitmq.shovel-config.dairynet.to-cloud-queue=dairynet.to.cloud
rabbitmq.shovel-config.dairynet.from-cloud-queue=cloud.to.dairynet

# Graceful shutdown configuration
server.shutdown=graceful
# Amount of time to wait for active requests to complete
spring.lifecycle.timeout-per-shutdown-phase=20s
# Grafana Alloy credentials endpoint
app.grafana.credentials-path-prefix=/grafana
app.grafana.loki-url=https://dnb-loki.dairynet.dev.gea.com/loki/api/v1/push

# grafana token refresh in minutes
app.dnc-token.expiry-time=15
app.dnc-token.refresh-fraction=0.5
# Fraction of a token's *remaining* lifetime to wait before sending a follow-up request.
# E.g. 0.5 with a 60-min token at 50% elapsed (30 min remaining) -> wait 15 min, retry at 75%
# A successful token-update response resets this cooldown immediately.
app.dnc-token.cooldown-fraction=0.5

dairy-net-data.cleanup-on-startup=true

# EDS Data Forwarding (FTDP-5898)
eds.mqtt-enabled=true
eds.rollover-minutes=60
eds.rollover-max-megabytes=200
eds.stash-retention-days=3
eds.storage-dir=./eds
eds.data-forwarding-base-url=${farm-site-connector-service.endpoint}
eds.upload-path=/api/dataforwarding/upload/eds


    a
