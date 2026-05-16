import re
from collections.abc import Iterable
from pathlib import Path


class KeyFilter:
    """Allowlist matcher for Redis keys/channels.

    Glob syntax: ``*`` matches any chars, ``?`` matches one char. All other
    characters (including ``[`` and ``]``) are matched literally, so paths like
    ``Colors[0]:Color`` work without escaping.
    """

    def __init__(self, patterns: Iterable[str]):
        self.exact: set[str] = set()
        self.regexes: list[re.Pattern[str]] = []
        for p in patterns:
            if '*' in p or '?' in p:
                escaped = re.escape(p).replace(r'\*', '.*').replace(r'\?', '.')
                self.regexes.append(re.compile(f'^{escaped}$'))
            else:
                self.exact.add(p)

    def matches(self, key: str) -> bool:
        if key in self.exact:
            return True
        return any(r.match(key) for r in self.regexes)

    def __len__(self) -> int:
        return len(self.exact) + len(self.regexes)


def load_filter(path: Path) -> KeyFilter:
    """Load patterns from a file (one per line; blank lines and ``#`` comments skipped)."""
    patterns: list[str] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            patterns.append(line)
    return KeyFilter(patterns)
