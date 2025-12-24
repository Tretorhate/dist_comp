import socket
import json
import uuid
import time
import datetime
import sys

# Default values (can be overridden via command line)
DEFAULT_SERVER_HOST = '44.200.84.170'  # Change this once if needed
SERVER_PORT = 5000
TIMEOUT = 2      # seconds
MAX_RETRIES = 3

def log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def rpc_call(server_host, method, params, retry_count=0):
    request_id = str(uuid.uuid4())
    request = {
        "request_id": request_id,
        "method": method,
        "params": params
    }
    
    log(f"Sending request {request_id}: {method}{params} (attempt {retry_count + 1}/{MAX_RETRIES + 1})")
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TIMEOUT)
            s.connect((server_host, SERVER_PORT))
            s.sendall(json.dumps(request).encode('utf-8'))
            
            data = s.recv(4096)
            if not data:
                raise Exception("No response data")
            
            response = json.loads(data.decode('utf-8'))
            log(f"Received response for {request_id}: {response}")
            
            if response.get("status") == "OK":
                return response["result"]
            else:
                return f"Error: {response.get('error', 'Unknown')}"
                
    except socket.timeout:
        log("Timeout waiting for response")
    except Exception as e:
        log(f"Connection/error: {e}")
    
    # Retry logic
    if retry_count < MAX_RETRIES:
        time.sleep(1)  # small backoff
        return rpc_call(server_host, method, params, retry_count + 1)
    else:
        return "Failed after retries: no response"

def main(server_host):
    log("RPC Client starting...")
    log(f"Target server: {server_host}:{SERVER_PORT}")
    
    # Example calls
    print("\n=== Calling add(5, 7) ===")
    result1 = rpc_call(server_host, "add", {"a": 5, "b": 7})
    print(f"Result: {result1}")
    
    print("\n=== Calling add(10, 20) ===")
    result2 = rpc_call(server_host, "add", {"a": 10, "b": 20})
    print(f"Result: {result2}")

if __name__ == "__main__":
    # Determine which host to use
    if len(sys.argv) > 1:
        server_host = sys.argv[1]
        print(f"Using server IP from command line: {server_host}")
    else:
        server_host = DEFAULT_SERVER_HOST
        print(f"Using default server IP: {server_host}")
    
    main(server_host)