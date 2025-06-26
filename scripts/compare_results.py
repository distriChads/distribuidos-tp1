from pathlib import Path
import os

def files_are_identical(file1, file2):
    return file1.read_bytes() == file2.read_bytes()

def compare_results():
    base_path = Path(__file__).parent.parent 
    known_dir = base_path / "client" / "known-good-results"
    clients_dir = base_path / "client" / "results"
    known_queries = list(known_dir.glob("query*.txt"))

    for client_id in os.listdir(clients_dir):
        client_dir_path = base_path / "client" / "results" / client_id
        client_queries = list(client_dir_path.glob(f"query*.txt"))
        print(f"========== Cliente {client_id} ==========")
        for known_file in known_queries:
            query_name = known_file.name
            
            matching_clients = [f for f in client_queries if f.name.endswith(query_name)]
            if not matching_clients:
                print(f"😴 No hay resultado del cliente para esta query: {query_name} 😴")
                continue

            for client_file in matching_clients:
                are_identical = files_are_identical(client_file, known_file)
                status = "😎 IGUALES 😎" if are_identical else "🥶 DIFERENTES 🥶"
                print(f"{client_file.name} vs {query_name}: {status}")
        sep = "=" * (len(client_id) + 30)
        print(sep)

if __name__ == "__main__":
    compare_results()