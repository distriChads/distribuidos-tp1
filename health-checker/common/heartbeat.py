import socket
import logging
import time
import threading

logger = logging.getLogger(__name__)

def heartbeat(port: int, ctx: threading.Event):
    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', port))
    sock.settimeout(1.0)  # Set timeout for non-blocking behavior
    
    try:
        logger.info(f"HeartBeat server listening on port {port}")
        
        while not ctx.is_set():
            try:
                # Receive data from socket
                data, addr = sock.recvfrom(5)
                if data[4] != 0:
                    logger.warning(f"Invalid message from {addr}: {data}")
                    continue
                if len(data) < 5:
                    logger.warning(f"Short read from {addr}: {data}")
                    continue
                
                # Print received message
                message = data[:-1].decode('utf-8') 
                logger.debug(f"Received from {addr}: {message}")
                
                # Send "OK" response back to the sender
                response = b"PONG\0"
                for i in range(3):
                    try:
                        total_sent = 0
                        while total_sent < len(response):
                            sent = sock.sendto(response[total_sent:], addr)
                            if sent == 0:
                                raise RuntimeError("Socket connection broken")
                            total_sent += sent
                        logger.debug(f"Sent response to {addr}")
                        break
                    except Exception as e:
                        logger.error(f"Error sending response (attempt {i+1}): {e}")
                        if i == 2:
                            logger.error("Failed to send response after 3 attempts")
                        time.sleep(2 + i * 1.2)
                        
            except socket.timeout:
                # No data available, continue
                continue
            except Exception as e:
                logger.error(f"Error reading from UDP: {e}")
                continue
                
    except Exception as e:
        logger.fatal(f"Error listening on UDP port: {e}")
    finally:
        sock.close()
        logger.info("HeartBeat server shutting down")