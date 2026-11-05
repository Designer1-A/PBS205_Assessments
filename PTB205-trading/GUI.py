import sys
import json
import tkinter as tk
from sendorder import connect
from datetime import datetime

# Dictionary of stocks, so other stocks prices are shown
STOCKS = ["XYZ", "ABC", "DEF"]
price_labels = {}

# Get new trade and set new stock price
def on_trade(channel, method, properties, body):
    trade = json.loads(body)
    stock = method.routing_key
    price = f"${trade['price']:.2f}"
    price_labels[stock].config(text=f"{stock}: {price}")
    history_list.insert(0, f"{stock}  {trade['buyer']} bought from {trade['seller']} at {price}")

# Every 200ms proceess new orders and looks for more 
def check_for_trades():
    connection.process_data_events(time_limit=0)
    window.after(200, check_for_trades)

# Run the file when is called
if __name__ == "__main__":
    # ---- RabbitMQ: subscribe to trades ----
    print("a")
    connection, channel = connect(sys.argv[1])
    channel.exchange_declare(exchange="trades", exchange_type="topic")
    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="trades", queue=queue_name, routing_key="*")
    channel.basic_consume(queue=queue_name, on_message_callback=on_trade, auto_ack=True)

    # Small window characteristics
    window = tk.Tk()
    window.title("XYZ Corp price board")
    
    for x in STOCKS:
        price_labels[x] = tk.Label(window, text=f"{x}: no trades yet", font=("Arial", 24))
        price_labels[x].pack(padx=40, pady=5)
    
    history_title = tk.Label(window, text="TRADE HISTORY" \
    ":", font=("Arial", 12))
    history_title.pack()

    history_list = tk.Listbox(window, width=50, height=10, font=("Consolas", 11))
    history_list.pack(padx=20, pady=(0, 20))

    check_for_trades()   
    window.mainloop()  

# References
# https://tkdocs.com/tutorial/eventloop.html
# https://tkdocs.com/tutorial/eventloop.html
# https://www.youtube.com/watch?v=6frbSDzvFVQ&list=PL7HAy5R0ehQXb2aFKOKyeCMequxyb5jzJ  

