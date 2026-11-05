import sys
import json
import pika

# Defines the connection point
def connect(endpoint):
    host, port = endpoint.split(":")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, int(port)))
    return connection, connection.channel()

# Show error if action is not BUY or SELL
def send_order(user, endpoint, action, quantity, price):
    action = action.upper()
    if action not in("BUY", "SELL"):
        raise ValueError("Action can only be BUY or SELL")
    
    # Set requirments for input
    order = { 
            "user": user,
            "action": action,
            "quantity": int(quantity),
            "price": float(price)
    }

    connection, channel = connect(endpoint)
    
    channel.exchange_declare(exchange = "orders", 
                             exchange_type = "topic")
    
    channel.basic_publish(exchange = "orders", 
                          routing_key = "XYZ", 
                          body=json.dumps(order))
    connection.close()
    return order

# Run the file when is called
if __name__ == "__main__":
    try:
        order = send_order(*sys.argv[1:6])
        print("Order sent:", order)
    except ValueError as error:
        sys.exit(error)

# References
# https://docs.python.org/3/builtins/stdtypes.html#str.split
# https://docs.python.org/3/tutorial/datastructures.html#tuples-and-sequences
# https://pika.github.io/pika/latest/modules/adapters/blocking/
# https://www.rabbitmq.com/docs/channels 
# https://www.rabbitmq.com/docs/exchanges 
# https://www.rabbitmq.com/tutorials/tutorial-five-python
# https://docs.python.org/3/library/json.html#json.dumps 


# open rabbit mq: docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management