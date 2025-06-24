from collections import defaultdict


class HasherContainer:
    def __init__(self, dict_node_type_positions: dict[str, int]):
        """
        Initializes the object with buffers for each node type.

        Args:
            dict_node_type_positions (dict[str, int]): 
                A dictionary mapping node types (as strings) to the number of positions (as integers) for each node type.

        Attributes:
            buffers (dict[str, list[list]]): 
                A dictionary where each key is a node type and each value is a list of empty lists, 
                with the number of lists corresponding to the number of positions specified for that node type.
        """
        self.buffers = {}
        for node_type, positions in dict_node_type_positions.items():
            self.buffers[node_type] = [[] for _ in range(positions)]

    def append_to_node(self, movie_id: int, data: str):
        """
        Appends the given data to the appropriate buffer shard for each node type based on the movie ID.

        Behavior:
            For each node type in self.buffers, computes the shard index by taking the modulo of movie_id
            with the number of shards for that node type, and appends the data to the corresponding buffer.
        """
        for node_type in self.buffers:
            shard_idx = movie_id % len(self.buffers[node_type])
            self.buffers[node_type][shard_idx].append(data)

    def get_buffers(self) -> dict[str, dict[int, str]]:
        """
        Aggregates and clears the contents of the internal buffers.

        Returns:
            dict[str, dict[int, str]]: A dictionary mapping each node type to another dictionary,
            which maps shard indices to their concatenated string contents.
        """
        results = defaultdict(dict)

        for node_type, lists in self.buffers.items():
            for i, shard in enumerate(lists):
                if not shard:
                    continue
                results[node_type][i] = "".join(shard)
                self.buffers[node_type][i] = []

        return dict(results)
