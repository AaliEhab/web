import websocket
import json
import time
import threading

class BinanceWebSocketTicker:
    def __init__(self, coin='BTC', pair='USDT', file_name='ticker_data.json', reconnect_interval=3600):
        self.coin = coin.upper()
        self.pair = pair.upper()
        self.symbol = f"{self.coin}{self.pair}".lower()  # e.g., 'btcusdt'
        self.file_name = file_name
        self.reconnect_interval = reconnect_interval  # Reconnect interval in seconds
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@ticker"
        self.current_ws = None  # Active WebSocket connection
        self.next_ws = None  # Next WebSocket connection for seamless reconnect
        self.last_price = None  # Store last price to avoid duplicates

    def on_message(self, ws, message):
        """Callback when a new ticker message is received."""
        data = json.loads(message)
        price = float(data['c'])  # 'c' represents the current close price
        timestamp = int(time.time())

        # Save only if price has changed
        if price != self.last_price:
            self.save_to_file(timestamp, price)
            print(f"Saved ticker: {{timestamp: {timestamp}, price: {price}}}")
            self.last_price = price

    def save_to_file(self, timestamp, price):
        """Appends the ticker data to the file in {timestamp: price} format."""
        data = {timestamp: price}
        try:
            with open(self.file_name, 'a') as file:
                json.dump(data, file)
                file.write('\n')  # Newline to separate each entry
        except IOError as e:
            print(f"Error writing to file: {e}")

    def connect(self, ws_name):
        """Connect to the WebSocket and set up the message handling."""
        print(f"Connecting to WebSocket for {self.symbol} ticker...")
        ws = websocket.WebSocketApp(self.ws_url,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

        # Set either current_ws or next_ws based on the connection name
        if ws_name == "current_ws":
            self.current_ws = ws
        elif ws_name == "next_ws":
            self.next_ws = ws

        ws.run_forever()

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket closed")

    def run(self):
        """Main method to handle continuous connection and reconnection."""
        while True:
            # Start the initial WebSocket connection
            if not self.current_ws:
                ws_thread = threading.Thread(target=self.connect, args=("current_ws",))
                ws_thread.start()
                ws_thread.join()  # Wait until the first connection is established
            
            # Wait until the reconnect interval before starting the next WebSocket
            time.sleep(self.reconnect_interval - 5)  # Start the next connection 5 seconds early
            
            # Start the next WebSocket connection
            next_ws_thread = threading.Thread(target=self.connect, args=("next_ws",))
            next_ws_thread.start()
            next_ws_thread.join()  # Wait until the new connection is established

            # Close the previous WebSocket connection after the new one is ready
            if self.current_ws:
                print("Closing the previous WebSocket connection...")
                self.current_ws.close()
                self.current_ws = self.next_ws
                self.next_ws = None

# Example usage
if __name__ == '__main__':
    # Fetch BTC/USDT ticker, save to 'btc_usdt_ticker.json', reconnect every hour
    ticker = BinanceWebSocketTicker(coin='BTC', pair='USDT', file_name='btc_usdt_ticker.json', reconnect_interval=3600)
    ticker.run()
