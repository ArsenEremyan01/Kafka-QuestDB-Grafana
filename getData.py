from kafkaHelper import initProducer, produceRecord
import websocket
import threading

producer = initProducer()


class SocketConn(websocket.WebSocketApp):
    def __init__(self, url, producer, topic):
        super().__init__(url=url, on_open=self.on_open)
        self.producer = producer
        self.topic = topic
        self.on_message = lambda ws, msg: self.send_message(msg)
        self.on_error = lambda ws, e: print("Error", e)
        self.on_close = lambda ws: print("Closing")
        self.run_forever()

    def on_open(self, ws):
        print("Websocket was opened")

    def send_message(self, msg):
        produceRecord(msg, self.producer, self.topic)


socket_conn = SocketConn('wss://stream.binance.com:9443/ws/btcusdt@aggTrade', producer, topic="binance")

threading.Thread(target=socket_conn.run_forever).start()
