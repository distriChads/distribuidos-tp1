from collections import defaultdict
import logging


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
                with the number of lists corresponding to the specified positions for that node type.
        """
        self.buffers = {}
        for node_type, positions in dict_node_type_positions.items():
            self.buffers[node_type] = [[] for _ in range(positions)]

    def append_to_node(self, movie_id: int, data: str):
        """
        Appends the given data to the appropriate buffer shard for each node type based on the movie ID.

        Args:
            movie_id (int): The unique identifier for the movie, used to determine the shard index.
            data (str): The data to append to the selected buffer shard.

        Details:
            For each node type in self.buffers, calculates the shard index by taking the modulo of movie_id
            with the number of shards for that node type. Appends the data to the corresponding shard.
        """
        for node_type in self.buffers:
            shard_idx = movie_id % len(self.buffers[node_type])
            self.buffers[node_type][shard_idx].append(data)

    def get_buffers(self) -> dict[str, dict[int, str]]:
        """
        Aggregates and retrieves the current buffer contents for each node type and shard.

        Returns:
            dict[str, dict[int, str]]: A dictionary where each key is a node type (str),
            and each value is another dictionary mapping shard indices (int) to the
            concatenated string contents of their respective buffers. After retrieval,
            the buffers for each shard are cleared.
        """
        results = defaultdict(dict)

        for node_type, lists in self.buffers.items():
            for i, shard in enumerate(lists):
                if not shard:
                    continue
                results[node_type][i] = "".join(shard)
                self.buffers[node_type][i] = []

        return dict(results)
