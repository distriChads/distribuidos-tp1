from typing import Optional
from types import FrameType
import socket
import signal
import logging
import subprocess
import time
import asyncio
from common.communication import Socket
import traceback

logger = logging.getLogger(__name__)

class HealthChecker:
    def __init__(self, ping_interval: int, services: list[str], max_concurrent_health_checks: int, grace_period: int):
        self.ping_interval = ping_interval
        self.services = services
        self.grace_period = grace_period
        self._running = True
        self._sockets = {} # service: socket
        self.semaphore = asyncio.Semaphore(max_concurrent_health_checks)
        self.tasks = []
        
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        for task in self.tasks:
            task.cancel()
        self._running = False
        for sock in self._sockets.values():
            if sock:
                sock.close()
                
    async def __reboot_container(self, service: str):
        addr, _port = service.split(":")
        logger.info(f"Rebooting container {addr}")
        while True:
            try:
                # Use the mounted docker-compose.yaml file explicitly
                #subprocess.run(["docker", "compose", "-f", "/app/docker-compose.yaml", "up", "-d", addr], check=True)
                subprocess.run(["docker", "start", addr], check=True)
                break
            except subprocess.CalledProcessError as e:
                logger.warning(f"Error rebooting container {addr}: {e}")
                await asyncio.sleep(self.ping_interval)
        
    async def __health_check_task(self, service: str):
        try:
            while self._running:
                sock = None
                async with self.semaphore:
                    attempts = 0
                    while attempts < 3 and self._running:
                        try:
                            sock = Socket()
                            self._sockets[service] = sock
                            # Send PING message to service
                            addr, port = service.split(":")
                            await sock.send_to("PING", addr, int(port))
                            
                            # Listen for response
                            data = await sock.read()
                            if data == "PONG":
                                logger.info(f"Service {service} is healthy")
                                if sock:
                                    sock.close()
                                    sock = None
                                    self._sockets[service] = None
                                break
                            else:
                                raise ValueError(f"Service {service} sent unexpected response: {data}")
                        except socket.timeout:
                                logger.error(f"Service {service} did not respond (timeout)")
                        except Exception as e:
                            logger.error(f"Error receiving response from {service}: {e}")
                        attempts += 1
                        if sock:
                            sock.close()
                            sock = None
                            self._sockets[service] = None
                if attempts == 3:
                    await self.__reboot_container(service)
                    await asyncio.sleep(self.grace_period / 2.0)
                await asyncio.sleep(self.ping_interval)
        except asyncio.CancelledError:
            logger.info(f"Health check task for {service} cancelled")
            return
        except Exception as e:
            logger.error(f"Error in health check task for {service}: {traceback.format_exc()}")
            return
        
    async def run(self):
        logger.info(f"Sleeping for {self.grace_period} seconds before starting health checks")
        await asyncio.sleep(self.grace_period)
        logger.info(f"Grace period of {self.grace_period} seconds has passed. Starting health checks")
        for service in self.services:
            self.tasks.append(asyncio.create_task(self.__health_check_task(service)))
        await asyncio.gather(*self.tasks)