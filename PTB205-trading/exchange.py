import sys
import json
from sendorder import connect

# Main function, runs the program and fiind matches
def live_order(channel, method, properties,body):
    order = json.loads(body)
    print("A new order has been recieved:", order)

    match = find_match(order)
    if match:
        if order["action"] == "BUY":
            buyer, seller = order["user"], match["user"]
        else:
            buyer, seller = match["user"], order["user"]
        trade = {"buyer": buyer, "seller": seller, "quantity": 100, "price": match["price"]}
        channel.basic_publish(exchange="trades", routing_key="XYZ", body=json.dumps(trade))
        print("Trade: ", trade)
    else: 
        add_to_book(order)

BUY_prices = []
SELL_prices = []

# Function to get last traded price
def getting_price(order):
    return order["price"]

# If no match found, it add it to the book
def add_to_book(order):
    if order["action"] == "BUY":
        BUY_prices.append(order)
        BUY_prices.sort(key = getting_price, reverse = True)
    else:
        SELL_prices.append(order)
        SELL_prices.sort(key = getting_price)

# Function to match BUY and SELL offers
def find_match(order):
    if order["action"] == "BUY":
        if SELL_prices and SELL_prices[0]["price"] <= order["price"]:
            return SELL_prices.pop(0)
    else:
        if BUY_prices and BUY_prices[0]["price"] >= order["price"]:
            return BUY_prices.pop(0)
    return None

# Run the file when is called
if __name__ == "__main__":
    connection, channel = connect(sys.argv[1])
    channel.exchange_declare(exchange = "orders", exchange_type = "topic")
    channel.queue_declare(queue = "exchange_orders")
    channel.queue_bind(exchange = "orders", queue = "exchange_orders", routing_key = "XYZ")
    channel.basic_consume(queue = "exchange_orders", on_message_callback = live_order, auto_ack = True)
    channel.exchange_declare(exchange="trades", exchange_type="topic")
    print("Exchange is ready and waiting for orders..." \
          "Press Ctrl+C to close.")
    channel.start_consuming()

# Reference
# https://www.cloudamqp.com/blog/part2-3-rabbitmq-for-beginners-example-and-sample-code-python 