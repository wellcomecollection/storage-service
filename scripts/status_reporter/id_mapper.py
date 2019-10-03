class IDMapper:
    """
    Holds a mapping between old and new IDs.
    """

    def __init__(self):
        self.new_to_old_map = {}
        self.old_to_new_map = {}

    def id_matches(self, old_id, new_id):
        try:
            return (
                self.new_to_old_map[new_id] == old_id
                and self.old_to_new_map[old_id] == new_id
            )
        except KeyError:
            assert new_id not in self.new_to_old_map
            assert old_id not in self.old_to_new_map

            self.new_to_old_map[new_id] = old_id
            self.old_to_new_map[old_id] = new_id

            return True
