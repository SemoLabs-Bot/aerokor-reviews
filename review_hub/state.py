from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Set

from review_hub.lock import file_lock


@dataclass
class TextSet:
    path: str

    def load(self) -> Set[str]:
        if not os.path.exists(self.path):
            return set()
        with open(self.path, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}

    def add_many(self, items: Iterable[str]):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        lock_path = self.path + ".lock"
        with file_lock(lock_path):
            existing = self.load()
            new_items = [i for i in items if i and i not in existing]
            if not new_items:
                return 0
            with open(self.path, "a", encoding="utf-8") as f:
                for i in new_items:
                    f.write(i + "\n")
            return len(new_items)
