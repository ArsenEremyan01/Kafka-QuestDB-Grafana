from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    print("Enter message to be sent:")
    msg = input()
    if msg == "quit":
        break
    producer.send('test', msg.encode('utf-8'))
    print("Sending msg \"{}\"".format(msg))
    print("Message sent!")
