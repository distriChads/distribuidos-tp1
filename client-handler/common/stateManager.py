import json
import logging
import os

logger = logging.getLogger(__name__)


class StateManager:
    def __init__(self, state_file_path):
        self.state_file_path = state_file_path
        self.aux_state_file_path = state_file_path.replace(".json", "_aux.json")
        self.state = {}
        self.load_existing_state(use_aux_file=False)

    def load_existing_state(self, use_aux_file=False):
        loaded = False
        try:
            with open(self.state_file_path, "r") as f:
                self.state = json.load(f)
            logger.info(f"Loaded state from {self.state_file_path}")
            loaded = True
        except FileNotFoundError:
            logger.info(f"No state file found at {self.state_file_path}")
        except Exception as e:
            logger.error(f"Error loading state from {self.state_file_path}: {e}")
        finally:
            if loaded:
                return
            if use_aux_file:
                logger.info("No valid state file found, creating new state")
                self.state = {}
                return
            logger.info(f"Loading state from {self.aux_state_file_path} as fallback")
            self.load_existing_state(use_aux_file=True)

    def commit_state(self):
        with open(self.aux_state_file_path, "w") as f:
            json.dump(self.state, f)
        
        try:
            os.rename(self.aux_state_file_path, self.state_file_path)
            logger.debug(f"Committed state from {self.aux_state_file_path} to {self.state_file_path}")
        except Exception as e:
            logger.error(f"Error committing state: {e}")
            raise e

    def add_client(self, client_id):
        self.state[client_id] = "PROCESSING"
        self.commit_state()

    def delete_client(self, client_id):
        self.state.pop(client_id, None)
        self.commit_state()
        
    def get_all_clients(self):
        return list(self.state.keys())
    
    def remove_all_clients(self):
        self.state = {}
        self.commit_state()
