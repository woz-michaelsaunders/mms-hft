"""
Simple kdb+ IPC client for basic queries
Implements minimal kdb+ IPC protocol to avoid qpython compatibility issues
"""

import socket
import struct
from typing import Any, Optional, List, Dict
import numpy as np
from datetime import datetime

class KdbClient:
    """Simple kdb+ IPC client"""
    
    def __init__(self, host: str = "localhost", port: int = 5000):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
    
    def connect(self):
        """Connect to kdb+ server"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(10)
            self.sock.connect((self.host, self.port))
        except Exception as e:
            raise ConnectionError(f"Failed to connect to kdb+ at {self.host}:{self.port}: {e}")
    
    def close(self):
        """Close connection"""
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None
    
    def execute(self, query: str) -> Any:
        """Execute a q query and return result"""
        if not self.sock:
            self.connect()
        
        # Serialize query string
        query_bytes = query.encode('utf-8')
        
        # kdb+ IPC message format: [endian:1][msg_type:1][compression:1][reserved:1][length:4][data]
        # For sync string execution: endian=1 (little), msg_type=0 (sync), compression=0
        msg_type = 0  # sync
        compression = 0
        reserved = 0
        length = len(query_bytes)
        
        # Build message
        message = struct.pack('<BBBBi', 1, msg_type, compression, reserved, length) + query_bytes
        
        try:
            # Send message
            self.sock.sendall(message)
            
            # Read response
            # First 8 bytes: [endian:1][msg_type:1][compression:1][reserved:1][length:4]
            header = self.sock.recv(8)
            if len(header) < 8:
                raise ValueError("Incomplete response header")
            
            endian, msg_type, compression, reserved, length = struct.unpack('<BBBBi', header)
            
            # Read data
            data = b''
            while len(data) < length:
                chunk = self.sock.recv(length - len(data))
                if not chunk:
                    raise ValueError("Incomplete response data")
                data += chunk
            
            # Parse response - for now, return as string
            # Full parsing would require implementing kdb+ serialization format
            # For simplicity, we'll use a different approach - execute via qpython if available
            # or return raw data for simple cases
            
            # Try to decode as string (for simple responses)
            try:
                return data.decode('utf-8', errors='ignore')
            except:
                return data
                
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")

