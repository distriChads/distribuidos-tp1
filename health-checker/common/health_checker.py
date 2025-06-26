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
    """
    Class for service health checker.
    Sends pings to services and restarts unresponsive services.
    
    :param ping_interval: interval between pings in seconds
    :param services: list of services (host:port) to check
    :param max_concurrent_health_checks: maximum number of concurrent health checks
    :param grace_period: grace period before starting health checks in seconds
    :param max_retries: maximum number of ping retries before rebooting a service
    :param skip_grace_period: whether to skip the grace period and start health checks immediately
    """

    def __init__(self, ping_interval: int, services: list[str], max_concurrent_health_checks: int, grace_period: int, max_retries: int, skip_grace_period: bool):
        self.ping_interval = ping_interval
        self.services = services
        self.grace_period = grace_period
        self.max_retries = max_retries
        self.skip_grace_period = skip_grace_period
        self._running = True
        self._sockets = {} # service: socket
        self.semaphore = asyncio.Semaphore(max_concurrent_health_checks)
        self.tasks = []
        
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        """
        Handles graceful shutdown and resource cleanup of the health checker.
        """
        for task in self.tasks:
            task.cancel()
        self._running = False
        for sock in self._sockets.values():
            if sock:
                sock.close()
                
    async def __reboot_container(self, service: str):
        """
        Reboots a service container.
        Assumes the container name is the same as the host name.
        """
        addr, _ = service.split(":")
        logger.info(f"Rebooting container {addr}")
        while self._running:
            try:
                subprocess.run(["docker", "start", addr], check=True, env={"SKIP_GRACE_PERIOD": "true"})
                break
            except subprocess.CalledProcessError as e:
                logger.warning(f"Error rebooting container {addr}: {e}")
                await asyncio.sleep(self.ping_interval)
        
    async def __health_check_task(self, service: str):
        """
        Task for checking the health of a service.
        Sends a PING message to the service and waits for a PONG response.
        If the service does not respond after a certain number of retries, it reboots the container.
        If the service responds successfully, it waits until the next ping interval.
        """
        try:
            while self._running:
                sock = None
                async with self.semaphore:
                    attempts = 0
                    while attempts < self.max_retries and self._running:
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
                if attempts == self.max_retries:
                    await self.__reboot_container(service)
                    await asyncio.sleep(self.grace_period * 0.8)
                await asyncio.sleep(self.ping_interval)
        except asyncio.CancelledError:
            logger.info(f"Health check task for {service} cancelled")
            return
        except Exception as e:
            logger.error(f"Error in health check task for {service}: {traceback.format_exc()}")
            return
        
    async def run(self):
        """
        Main loop for the health checker.
        Launches and awaits all health check tasks.
        """
        if not self.skip_grace_period:
            logger.info(f"Sleeping for {self.grace_period} seconds before starting health checks")
            await asyncio.sleep(self.grace_period)
            logger.info(f"Grace period of {self.grace_period} seconds has passed. Starting health checks")
        else:
            logger.info(f"Skipping grace period. Starting health checks immediately")
        for service in self.services:
            self.tasks.append(asyncio.create_task(self.__health_check_task(service)))
        await asyncio.gather(*self.tasks)