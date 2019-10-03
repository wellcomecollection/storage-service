class IDMapper:
    """
    Holds a mapping between old and new IDs.
    """

    def __init__(self):
        self.id_map = {}

    def id_matches(self, old_id, new_id):
        try:
            return self.id_map[old_id] == new_id
        except KeyError:
            self.id_map[old_id] = new_id
            return True
