import json
from kafkaHelper import initConsumer
import csv
import os

consumer = initConsumer('binance')

csv_file_path = '../kafkaProject/trades.csv'

if os.path.isfile(csv_file_path):
    print("trades.csv already exists... deleting...")
    os.remove(csv_file_path)

print("CSV initialized")

try:
    with open(csv_file_path, 'a', newline='') as f:
        fWriter = csv.writer(f)
        fWriter.writerow(
            ['Event Type', 'Event Time', 'Symbol', 'Aggregate Trade ID', 'Price', 'Quantity', 'First Trade ID',
             'Last Trade ID', 'Trade Time', 'Is Buyer the Market Maker?', 'Ignore'])

        for message in consumer:
            print("Received message:", message.value)
            data = json.loads(message.value)
            event_type = data['e']
            event_time = str(data['E'])
            symbol = data['s']
            aggregate_trade_id = data['a']
            price = data['p']
            quantity = data['q']
            first_trade_id = data['f']
            last_trade_id = data['l']
            trade_time = data['T']
            is_buyer_market_maker = data['m']
            ignore = data['M']

            fWriter.writerow(
                [event_type, event_time, symbol, aggregate_trade_id, price, quantity, first_trade_id, last_trade_id,
                 trade_time, is_buyer_market_maker, ignore])
except Exception as e:
    print("Error:", e)
