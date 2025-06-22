class HasherContainer:
    def __init__(self, dict_node_type_positions: int):
        self.buffers = {}
        for node_type, positions in dict_node_type_positions.items():
            self.buffers[node_type] = [[] for _ in range(positions)]

    def append_to_node(self, movie_id: int, data: str):
        for node_type in self.buffers:
            shard_idx = movie_id % len(self.buffers[node_type])
            self.buffers[node_type][shard_idx].append(data)

    def get_buffers(self) -> list[list[str]]:
        results = []
        for _, lists in self.buffers.items():
            node_result = []
            for i in range(len(lists)):
                routing_key_result = "".join(lists[i])
                node_result.append(routing_key_result)
                lists[i] = []
            results.append(node_result)
        return results
