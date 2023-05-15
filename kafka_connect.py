from confluent_kafka import Producer, Consumer, KafkaError
import csv

csv_file = '/home/fundi/moringaschool/week8/airflow/airflow/customer_data.csv'
#cloud credentials
conf = {
'bootstrap.servers':'pkc-3w22w.us-central1.gcp.confluent.cloud:9092',
'security.protocol':'SASL_SSL',
'sasl.mechanisms':'PLAIN',
'sasl.username':'FSHYZB4NZQLLW6VJ',
'sasl.password':'//LUbnBCHlisMCya9rf1PX7Kh9XPi5KwxK2TB1s88bQaEwFiDemVV0IfKSwT6spN',
'group.id':'python_example'
}

#create producer instance
producer = Producer(conf)
#publish messages to the cloud kafka topic
topic_name = 'topic_0'
message = 'hello world kafka confluent'
producer.produce(topic_name, message.encode('utf-8'))
producer.flush()

#consumer instance
consumer = Consumer(conf)
#subscribe to your confluent cloud topic and consume messages
consumer.subscribe([topic_name])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("consumer error: {}".format(msg.error()))
        continue
    print("received message: {}".format(msg.value().decode('utf-8')))


# # Define function to produce messages to Kafka topic
# def produce_message(topic, message):
#     producer.produce(topic=topic, value=message)
#     producer.flush()

# # Read CDR data from CSV file
# with open(csv_file, 'r') as csvfile:
#     cdr_reader = csv.reader(csvfile, delimiter=',')
#     next(cdr_reader) # Skip header row
#     for row in cdr_reader:
#       calling_number = row[1]
#       called_number = row[2]
#       call_duration = int(row[0])
#       #call_timestamp = int(row[6])

#       # Perform stream processing to calculate number of calls per calling number
#       # In this sample solution, we simply produce a message to the output Kafka topic
#       output_message = '{},{}'.format(calling_number, 1).encode('utf-8')
#       produce_message(topic_name, output_message)