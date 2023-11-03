from dataclasses import dataclass


@dataclass
class CardListChanges:
    num_added: int = 0
    num_removed: int = 0
    num_disabled: int = 0
    num_enabled: int = 0

    @property
    def num_changes(self) -> int:
        changes: int = 0
        changes += self.num_added
        changes += self.num_removed
        changes += self.num_disabled
        changes += self.num_enabled
        return changes
