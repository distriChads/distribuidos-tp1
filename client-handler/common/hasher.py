from collections import defaultdict
import logging


class HasherContainer:
    def __init__(self, dict_node_type_positions: dict[str, int]):
        self.buffers = {}
        for node_type, positions in dict_node_type_positions.items():
            self.buffers[node_type] = [[] for _ in range(positions)]

    def append_to_node(self, movie_id: int, data: str):
        for node_type in self.buffers:
            shard_idx = movie_id % len(self.buffers[node_type])
            self.buffers[node_type][shard_idx].append(data)

    def get_buffers(self) -> dict[str, dict[int, str]]:
        results = defaultdict(dict)

        for node_type, lists in self.buffers.items():
            for i, shard in enumerate(lists):
                if not shard:
                    continue
                results[node_type][i] = "".join(shard)
                self.buffers[node_type][i] = []

        return dict(results)
