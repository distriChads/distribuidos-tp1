import subprocess
import threading
import time
import random
import sys


def death_print(name):
    explosion = [
        "   *   *   *",
        f"*   💀 {name} 💀   *",
        " *   * * *   *"
    ]

    width = max(len(line) for line in explosion)

    print("\n" + "*" * width)
    print(explosion[0].center(width))
    print(explosion[1].center(width))
    print(explosion[2].center(width))
    print("*" * width + "\n")


def sad_print(name):
    message = f"El contenedor {name} no esta corriendo"
    width = len(message) + 4  # padding

    border = "." * width
    face = "😢"

    print("\n" + border)
    print(f". {face} {message} {face} .")
    print(border + "\n")


class ContainerController:

    def __init__(self):
        self.containers = self.get_active_containers()
        self.auto_thread = None
        self.auto_running = False

    def get_active_containers(self):
        running_containers = self.run_cmd(
            f"docker ps --format {{{{.Names}}}}").strip().splitlines()
        running_containers = [
            c for c in running_containers if not c.startswith("rabbit")]
        return running_containers

    def run_cmd(self, cmd):
        try:
            return subprocess.check_output(cmd, shell=True, text=True).strip()
        except subprocess.CalledProcessError:
            return ""

    def is_container_running(self, container_name):
        output = self.run_cmd(
            f"docker ps --filter name=^{container_name}$ --format {{{{.Names}}}}")
        return container_name.strip() == output.strip()

    def kill_containers(self, container_names: list[str]):
        containers = " ".join(container_names)
        self.run_cmd(f"docker stop {containers}")

    def kill_container(self, container_name):
        if self.is_container_running(container_name):
            self.run_cmd(f"docker stop {container_name}")
            death_print(container_name)
        else:
            sad_print(container_name)

    def kill_all_except_one_heartbeat(self):
        health_checker_skipped = False
        client_handler_skipped = False
        containers_to_kill = self.get_active_containers()
        print("Matando todos los contenedores en 3..2..1.. 💣💣💣💥💥💥💣💣💣")
        time.sleep(1)

        for container_name in self.containers:
            if container_name.startswith("health-checker") and not health_checker_skipped:
                containers_to_kill.remove(container_name)
                health_checker_skipped = True
            elif container_name.startswith("client-handler") and not client_handler_skipped:
                containers_to_kill.remove(container_name)
                client_handler_skipped = True
            if health_checker_skipped and client_handler_skipped:
                break

        self.kill_containers(containers_to_kill)
        print(
            "Todos los contenedores han sido asesinados, excepto un heartbeat. 💣💣💣💥💥💥💣💣💣\n")

    def kill_with_prefix(self, prefix):
        for container_name in self.containers:
            if container_name.startswith(prefix):
                self.kill_container(container_name)

    def auto_kill(self, x, y):
        self.auto_running = True
        while self.auto_running:
            alive_containers = [
                container_name for container_name in self.containers if self.is_container_running(container_name)]
            if not alive_containers:
                print("No hay mas contenedores vivos")
                self.stop_auto_kill()
            containers_to_kill = random.sample(
                alive_containers, min(y, len(alive_containers)))
            for container_name in containers_to_kill:
                self.kill_container(container_name)
            time.sleep(x)

    def start_auto_kill(self, x, y):
        if self.auto_thread and self.auto_thread.is_alive():
            print("Ya esta auto en ejecucion, frenalo primero con stop")
            return
        self.auto_thread = threading.Thread(
            target=self.auto_kill, args=(x, y), daemon=True)
        self.auto_thread.start()

    def stop_auto_kill(self):
        self.auto_running = False
        if self.auto_thread and self.auto_thread.is_alive():
            self.auto_thread.join()
        print("Auto detenido")

    def prompt_loop(self):
        print(f"Contenedores cargados: {self.containers}\n")
        while True:
            self.print_helper()
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
            elif cmd == "mostrar":
                running_containers = self.run_cmd(
                    f"docker ps --format {{{{.Names}}}}").strip().splitlines()
                print(f"Contenedores corriendo: {running_containers}")
                running_containers = [
                    c for c in running_containers if not c.startswith("rabbit")]
            else:
                print("Comando inexistente")

    def print_helper(self):
        print("\nComandos disponibles:")
        print("auto X Y - Matar X contenedores cada Y segundos")
        print("stop - Detener el auto kill")
        print("bomba - Matar todos los contenedores excepto uno con nombre hearth-beat")
        print("matar <nombre> - Matar un contenedor especifico")
        print("matar_varios <prefijo> - Matar todos los contenedores con un prefijo especifico")
        print("mostrar - Mostrar contenedores corriendo")
        print("Ctrl+C o Ctrl+D - Salir del programa\n")


if __name__ == "__main__":
    controller = ContainerController()
    controller.prompt_loop()
