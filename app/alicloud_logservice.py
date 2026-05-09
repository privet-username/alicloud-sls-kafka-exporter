from aliyun.log.consumer import ConsumerProcessorBase, LogHubConfig, CursorPosition, ConsumerWorker
from aliyun.log.pulllog_response import PullLogResponse
from datetime import datetime
from confluent_kafka import Producer
import os
import json

class SyncData(ConsumerProcessorBase):
    def __init__(self):
        super(SyncData, self).__init__()
        
        bootstrap_servers = os.environ.get('KAFKA_SERVERS')
        self.topic = os.environ.get('KAFKA_TOPIC')
        
        # SSL settings for Kafka. From envs or filenames
        ssl_ca = os.environ.get('KAFKA_SSL_CA', 'kafka-ca.pem')
        ssl_cert = os.environ.get('KAFKA_SSL_CERT', 'producer-cert.pem')
        ssl_key = os.environ.get('KAFKA_SSL_KEY', 'producer-key.pem')

        self.producer = Producer({
            'metadata.broker.list': bootstrap_servers,
            'security.protocol': 'SSL',
            'ssl.ca.location': ssl_ca,
            'ssl.certificate.location': ssl_cert,
            'ssl.key.location': ssl_key
        })

        # Metadata for logs
        self.log_host = os.environ.get('LOG_HOST', 'actiontrail-logs-organization')
        self.log_source = os.environ.get('LOG_SOURCE', 'alicloud-actiontrail-logs-exporter')
        self.log_sourcetype = os.environ.get('LOG_SOURCETYPE', 'actiontrail_actiontrail-logs-organization')

    def process(self, log_groups, check_point_tracker):
        logs = PullLogResponse.loggroups_to_flattern_list(log_groups, time_as_str=True, decode_bytes=True)

        for log in logs:
            event = {}
            log.pop('__time__', None)
            log.pop('__topic__', None)
            log.pop('__source__', None)
            
            event['log_message'] = log.get(u'event', '')
            event['log_host'] = self.log_host
            event['log_source'] = self.log_source
            event['log_utc_time_ingest'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            event['log_sourcetype'] = self.log_sourcetype

            data = json.dumps(event, sort_keys=True)
            self.producer.produce(self.topic, data)
            
        self.producer.flush()
        self.save_checkpoint(check_point_tracker)


def get_monitor_option():
    # Requered parameters
    endpoint = os.environ['ALICLOUD_ENDPOINT']
    accessKeyId = os.environ['ALICLOUD_KEY_ID']
    accessKey = os.environ['ALICLOUD_KEY']
    project = os.environ['ALICLOUD_PROJECT']
    logstore = os.environ['ALICLOUD_LOGSTORE']
    
    # Optional parameters with default fields
    consumer_group = os.environ.get('CONSUMER_GROUP', 'alicloud-actiontrail-logs-exporter')
    consumer_name = os.environ.get('CONSUMER_NAME', 'alicloud-actiontrail-logs-exporter')
    cursor_start_time = os.environ.get('CURSOR_START_TIME', '2018-12-26 0:0:0')
    heartbeat_interval = int(os.environ.get('HEARTBEAT_INTERVAL', 20))
    data_fetch_interval = int(os.environ.get('DATA_FETCH_INTERVAL', 1))

    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group, consumer_name,
                          cursor_position=CursorPosition.SPECIAL_TIMER_CURSOR,
                          cursor_start_time=cursor_start_time,
                          heartbeat_interval=heartbeat_interval,
                          data_fetch_interval=data_fetch_interval)

    return option


def main():
    option = get_monitor_option()
    worker = ConsumerWorker(SyncData, option)
    worker.start(join=True)


if __name__ == '__main__':
    main()
