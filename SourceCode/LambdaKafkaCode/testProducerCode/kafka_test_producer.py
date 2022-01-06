from confluent_kafka import Producer
from time import sleep
import json
import time
import random
from random import randrange
from datetime import datetime, timedelta, timezone

# -------------------------------------------
# Kafka configuration
# -------------------------------------------
def config_listener(broker):
    # Start listener
    producer_config = {
        'bootstrap.servers': broker,
        # SSL configs
        "security.protocol": "SSL",
        "ssl.ca.location": "ca_certificate_chain.pem",
        "ssl.certificate.location": "certificate.pem",
        "ssl.key.location": "key.pem"
    }
    return Producer(producer_config)

# -------------------------------------------
# Log Generator Class
# -------------------------------------------
class DataGenerator():
    def __init__(self):
        pass

    def app_name_generator(self):
        apps = ['APP1',
         'APP2',
         'APP3',
         'APP4']
        self.app_name = random.choice(apps)
        return self.app_name

    def api_name_generator(self):
        api_names = ['api-name-1', 'api-name-2', 'api-name-3',
                     'api-name-4']
        self.api_name = random.choice(api_names)
        return self.api_name

    def status_code_generator(self, anomaly=False):
        status_codes = ['200 OK', '404', '500']
        normal_weights = [1, 0, 0]
        anomaly_weights = [0, 0.5, 0.5]
        weights = anomaly_weights if anomaly else normal_weights

        self.status_code = random.choices(status_codes, weights)[0]
        return self.status_code

    def time_to_serve_generator(self, anomaly=False):
        time_population = [20, 22, 26, 82, 70, 25]
        normal_weights = [0.245, 0.245, 0.245, 0.01, 0.01, 0.245]
        anomaly_weights = [0.025, 0.025, 0.025, 0.45, 0.45, 0.025]
        weights = anomaly_weights if anomaly else normal_weights

        self.time_to_serve_request = random.choices(time_population, weights)[0]
        return self.time_to_serve_request

    def bytes_sent_generator(self, anomaly=False):
        bytes_sent_population = [1590, 1560, 1350, 1400, 3250, 4000]
        normal_weights = [0.245, 0.245, 0.245, 0.245, 0.01, 0.01]
        anomaly_weights = [0.025, 0.025, 0.025, 0.025, 0.45, 0.45]
        weights = anomaly_weights if anomaly else normal_weights

        self.bytes_sent = random.choices(bytes_sent_population, weights)[0]
        return self.bytes_sent

    def bytes_received_generator(self, anomaly=False):
        bytes_received_population = [458, 468, 534, 500, 490, 0]
        normal_weights = [0.2, 0.2, 0.2, 0.2, 0.19, 0.01]
        anomaly_weights = [0.025, 0.025, 0.025, 0.025, 0.025, 0.875]
        weights = anomaly_weights if anomaly else normal_weights

        self.bytes_received = random.choices(bytes_received_population, weights)[0]
        return self.bytes_received

    def datetime_generator(self):
        now = datetime.now(timezone.utc)
        now_minus_10 = datetime.now(timezone.utc) - timedelta(minutes=10)
        start = datetime.strptime(now_minus_10.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        end = datetime.strptime(now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%dT%H:%M:%S.%fZ')
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        self.datetime = (start + timedelta(seconds=random_second)).strftime(
            "%Y-%m-%dT%H:%M:%S.%f")
        self.datetime = self.datetime[:-3] + "Z"
        return self.datetime

data_generator = DataGenerator()

# -------------------------------------------
# Build mock log function
# -------------------------------------------

def generate_log(template_log):
    new_log = template_log
    anomaly_options = [True, False]
    anomaly_weights = [5, 95]

    anomaly = random.choices(anomaly_options, anomaly_weights, k=1)[0]
    print(anomaly)
    new_log['app_name'] = data_generator.app_name_generator()
    new_log['api_name'] = data_generator.api_name_generator()
    new_log['status_code'] = data_generator.status_code_generator(anomaly=anomaly)
    new_log['time_to_serve_request'] = data_generator.time_to_serve_generator(anomaly=anomaly)
    new_log['bytes_sent'] = data_generator.bytes_sent_generator(anomaly=anomaly)
    new_log['bytes_received'] = data_generator.bytes_received_generator(anomaly=anomaly)

    return new_log


def log_producer(broker, topic):
    producer = config_listener(broker)
    # Read log template
    f = open('template_event.json', )
    log_data = json.load(f)

    flush_counter = 0
    try:
        date = data_generator.datetime_generator()
        for i in range(0, 30000):
            new_log = generate_log(log_data)
            new_log["transaction_id"] = str('transaction' + str(i))
            new_log['datetime'] = date
            #print(new_log)
            producer.produce(topic, json.dumps(new_log))
            if flush_counter == 100:
                print("############## " + str(i))
                sleep(1)
                date = data_generator.datetime_generator()
                print(date, i)
                producer.flush()
                flush_counter = 0

            flush_counter += 1

    except Exception as ex:
        print("Kafka Exception : {}", ex)
    finally:
        print("closing producer")
        producer.flush()

# -------------------------------------------
# Main Invocation
# -------------------------------------------

broker = "b-1.msktest.qx098q.c21.kafka.us-east-1.amazonaws.com:9094,b-2.msktest.qx098q.c21.kafka.us-east-1.amazonaws.com:9094"
topic = "ExampleTopic"

my_producer = log_producer(broker, topic)