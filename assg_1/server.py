import socket
import json
import uuid
import threading
import time
import datetime

HOST = '0.0.0.0'  # Listen on all interfaces
PORT = 5000
FUNCTIONS = {}

def register(func):
    """Decorator to register remote functions"""
    FUNCTIONS[func.__name__] = func
    return func

@register
def add(a, b):
    return a + b

# Optional: add more functions
# @register
# def reverse_string(s):
#     return s[::-1]

def log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def handle_client(conn, addr):
    log(f"Connected by {addr}")
    with conn:
        data = conn.recv(4096)
        if not data:
            return
        
        try:
            request = json.loads(data.decode('utf-8'))
            req_id = request['request_id']
            method = request['method']
            params = request['params']
            
            log(f"Received request {req_id}: {method}{params}")
            
            if method not in FUNCTIONS:
                response = {
                    "request_id": req_id,
                    "status": "ERROR",
                    "error": "Unknown method"
                }
            else:
                # === For failure demo: artificial delay ===
                # Uncomment the next line to simulate slow server (timeout demo)
                # time.sleep(6)
                
                result = FUNCTIONS[method](**params)
                time.sleep(6)  # Simulate slow server
                response = {
                    "request_id": req_id,
                    "result": result,
                    "status": "OK"
                }
                
            # === For failure demo: drop response ===
            # Uncomment to simulate lost response
            # log("Simulating dropped response...")
            # return
            
            conn.sendall(json.dumps(response).encode('utf-8'))
            log(f"Sent response for {req_id}: {response['result'] if response['status'] == 'OK' else response['error']}")
            
        except Exception as e:
            log(f"Error handling request: {e}")
            error_resp = {
                "request_id": request.get('request_id', 'unknown'),
                "status": "ERROR",
                "error": str(e)
            }
            try:
                conn.sendall(json.dumps(error_resp).encode('utf-8'))
            except:
                pass

def main():
    log("Starting RPC server...")
    log(f"Registered functions: {list(FUNCTIONS.keys())}")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        log(f"Listening on port {PORT}")
        
        try:
            while True:
                conn, addr = s.accept()
                threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            log("Server shutting down...")

if __name__ == "__main__":
    main()