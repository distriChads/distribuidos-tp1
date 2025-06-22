import subprocess
import threading
import time
import random
import sys

class ContainerController:
    def __init__(self, filename):
        self.containers = self.load_containers_from_file(filename)
        self.auto_thread = None
        self.auto_running = False

    def load_containers_from_file(self, filename):
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]

    def run_cmd(self, cmd):
        try:
            return subprocess.check_output(cmd, shell=True, text=True).strip()
        except subprocess.CalledProcessError:
            return ""

    def is_container_running(self, container_name):
        output = self.run_cmd(f"docker ps --filter name=^{container_name}$ --format {{{{.Names}}}}")
        return container_name.strip() == output.strip()

    def kill_container(self, container_name):
        if self.is_container_running(container_name):
            self.run_cmd(f"docker stop {container_name}")
            print(f"Contenedor {container_name} matado")
        else:
            print(f"Contenedor {container_name} no esta corriendo")

    def kill_all_except_one_heartbeat(self):
        heartbeat_skipped = False
        for container_name in self.containers:
            if container_name.startswith("hearth-beat") and not heartbeat_skipped:
                heartbeat_skipped = True
                continue
            self.kill_container(container_name)

    def kill_with_prefix(self, prefix):
        for container_name in self.containers:
            if container_name.startswith(prefix):
                self.kill_container(container_name)

    def auto_kill(self, x, y):
        self.auto_running = True
        while self.auto_running:
            alive_containers = [container_name for container_name in self.containers if self.is_container_running(container_name)]
            if not alive_containers:
                print("No hay mas contenedores vivos")
                break
            containers_to_kill = random.sample(alive_containers, min(y, len(alive_containers)))
            for container_name in containers_to_kill:
                self.kill_container(container_name)
            time.sleep(x)

    def start_auto_kill(self, x, y):
        if self.auto_thread and self.auto_thread.is_alive():
            print("Ya esta auto en ejecucion, frenalo primero con stop")
            return
        self.auto_thread = threading.Thread(target=self.auto_kill, args=(x, y), daemon=True)
        self.auto_thread.start()

    def stop_auto_kill(self):
        self.auto_running = False
        if self.auto_thread and self.auto_thread.is_alive():
            self.auto_thread.join()
        print("Auto detenido")

    def prompt_loop(self):
        print(f"Contenedores cargados: {self.containers}")
        print(f"Todos los contenedores corriendo: {self.run_cmd(f"docker ps --format {{{{.Names}}}}")}")
        while True:
            try:
                cmd = input(">>> ").strip()
            except (EOFError, KeyboardInterrupt):
                self.stop_auto_kill()
                break

            if cmd.startswith("auto "):
                try:
                    _, x, y = cmd.split()
                    self.start_auto_kill(int(x), int(y))
                except ValueError:
                    print("Uso: auto X Y")
            elif cmd == "stop":
                self.stop_auto_kill()
            elif cmd == "bomba":
                self.kill_all_except_one_heartbeat()
            elif cmd.startswith("matar "):
                _, container_name = cmd.split(maxsplit=1)
                self.kill_container(container_name)
            elif cmd.startswith("matar_varios "):
                _, prefix = cmd.split(maxsplit=1)
                self.kill_with_prefix(prefix)
            else:
                print("Comando inexistente")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python chancho_loco.py archivo_con_nombre_contenedores.txt")
        sys.exit(1)
    
    controller = ContainerController(sys.argv[1])
    controller.prompt_loop()