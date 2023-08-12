params = {
  # crypto setup
  'currency_1': 'BTC', # Bitcoin
  'currency_2': 'ETH', # Ethereum
  'currency_3': 'LINK', # Chainlink
  'ref_currency': 'USD',
  'ma': 25,
  # api setup
}

config = {
  # kafka
  'kafka_broker': 'localhost:9092',
  'topic_1': 'topic_{0}'.format(params['currency_1']),
}