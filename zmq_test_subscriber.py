#!/usr/bin/env python3
"""
Simple ZeroMQ subscriber to test BTC_USDT order book updates.
Install: pip3 install --user pyzmq (or use venv)
"""
import zmq
import json
import time
import sys

try:
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    socket.subscribe(b"BTC_USDT")
    socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5 second timeout
    
    print("✅ Connected to ZeroMQ at tcp://localhost:5555")
    print("📡 Subscribed to BTC_USDT topic")
    print("⏳ Waiting for messages...\n")
    
    message_count = 0
    start_time = time.time()
    
    while True:
        try:
            # Receive multipart message: [topic, message_type, data]
            parts = socket.recv_multipart()
            
            if len(parts) >= 3:
                topic = parts[0].decode('utf-8')
                msg_type = parts[1].decode('utf-8')
                data_str = parts[2].decode('utf-8')
                
                message_count += 1
                elapsed = time.time() - start_time
                
                print(f"[{message_count}] @ {elapsed:.1f}s - Topic: {topic}, Type: {msg_type}")
                
                try:
                    data = json.loads(data_str)
                    if msg_type == "bids":
                        best_bid = data.get('best_bid', 'N/A')
                        spread = data.get('spread', 'N/A')
                        levels = len(data.get('levels', []))
                        print(f"    📊 Bids - Best: {best_bid}, Spread: {spread}, Levels: {levels}")
                    elif msg_type == "asks":
                        best_ask = data.get('best_ask', 'N/A')
                        spread = data.get('spread', 'N/A')
                        levels = len(data.get('levels', []))
                        print(f"    📊 Asks - Best: {best_ask}, Spread: {spread}, Levels: {levels}")
                    elif msg_type == "trades":
                        trade_count = data.get('trade_count', 0)
                        recent = len(data.get('trades', []))
                        print(f"    💹 Trades - Total: {trade_count}, Recent: {recent}")
                except json.JSONDecodeError:
                    print(f"    ⚠️  Invalid JSON data")
                
                print("")
                
        except zmq.Again:
            if message_count == 0:
                print("⏳ No messages yet, waiting...")
            continue
        except KeyboardInterrupt:
            print(f"\n\n📊 Summary:")
            print(f"   Total messages: {message_count}")
            print(f"   Duration: {time.time() - start_time:.1f}s")
            print(f"   Rate: {message_count / (time.time() - start_time):.2f} msg/s")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
            
except ImportError:
    print("❌ Error: pyzmq not installed")
    print("   Install with: pip3 install --user pyzmq")
    print("   Or use a virtual environment")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)

