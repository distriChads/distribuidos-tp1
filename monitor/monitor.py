import socket
import time
import subprocess


# NECESITO RECIBIR POR ENV VAR TODOS LOS NOMBRES DE LOS CHABONES ESTOS, FIJEMOSLE A TODOS EL EXPOSE: 9999/UDP EN EL DOCKER COMPOSE, BIEN HARDCODEADO Y FUE
NODES = {
    "group-by-overview-average-1": ("group-by-overview-average-1", 9999),
}

TIMEOUT = 2  
PING_INTERVAL = 5 

def udp_ping(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(TIMEOUT)
        try:
            sock.sendto(b"ping", (host, port))
            data, _ = sock.recvfrom(1024)   # Poner un recv como la gente
            return data == b"pong"
        except socket.timeout:
            return False

def restart_container(container_name):
    print(f"REINICIANDO {container_name}")
    subprocess.run(["docker", "restart", container_name])

def main():
    while True:
        for name, (host, port) in NODES.items():
            if udp_ping(host, port):
                print(f"{name} respondio")
            else:
                print(f"{name} no respondio")
                restart_container(name)
        time.sleep(PING_INTERVAL)

if __name__ == "__main__":
    main()