import re
import secrets
import string
import uuid
from typing import Any

import msgpack


class Obfuscator:
    """Replaces ticket/device IDs with fakes that can't collide with real ones.

    - Ticket IDs (UUID or ``at<hex>-<hex>-<hex>``) → random uuid4 (cached).
    - Empatica IDs (``E3`` prefix, alphanumeric) → 12-char alphanumeric starting
      with ``F`` (cached).
    - Empatica short IDs (value of ``Visitors:<ticket>:EmpaticaDeviceIDShort``)
      → ``F`` prepended to the original (deterministic, no cache).
    - Scent IDs (5-digit numbers starting with ``6``) → same digits with leading
      ``6`` replaced by ``7`` (deterministic, no cache needed).

    Scent's ``6`` prefix is too ambiguous to detect everywhere (random hex can
    look like a scent ID), so embedded scent substitution only runs when the
    surrounding key contains "Scent". UUID/at-id/Empatica are distinctive
    enough to substitute anywhere.
    """

    UUID_RE = re.compile(
        r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
    AT_ID_RE = re.compile(r'^at[a-f0-9]{6}-[a-f0-9]{18}-[a-f0-9]{8}$')
    EMPATICA_RE = re.compile(r'^E3[A-Z0-9]+$')
    SCENT_RE = re.compile(r'^6\d{4}$')

    KEY_VISITOR = re.compile(r'^Visitors:([^:]+)(:.*)?$')
    KEY_WATCH = re.compile(r'^Wearables:WatchDevices:([^:]+)(:.*)?$')
    KEY_SCENT = re.compile(r'^Wearables:Scent:([^:]+)(:.*)?$')

    # Combined regex so a fake UUID inserted by AT replacement isn't re-matched
    # as a UUID on a second pass.
    _EMBEDDED_BASE = (
        r'\b(?P<at>at[a-f0-9]{6}-[a-f0-9]{18}-[a-f0-9]{8})\b|'
        r'\b(?P<uuid>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\b|'
        r'\b(?P<empatica>E3[A-Z0-9]{4,18})\b'
    )
    EMBEDDED = re.compile(_EMBEDDED_BASE)
    EMBEDDED_WITH_SCENT = re.compile(
        _EMBEDDED_BASE + r'|\b(?P<scent>6\d{4})\b')

    _ALPHANUM = string.ascii_uppercase + string.digits

    def __init__(self) -> None:
        self.tickets: dict[str, str] = {}
        self.empaticas: dict[str, str] = {}

    def fake_ticket(self, real: str) -> str:
        if real not in self.tickets:
            self.tickets[real] = str(uuid.uuid4())
        return self.tickets[real]

    def fake_empatica(self, real: str) -> str:
        if real not in self.empaticas:
            self.empaticas[real] = 'F' + \
                ''.join(secrets.choice(self._ALPHANUM) for _ in range(11))
        return self.empaticas[real]

    @staticmethod
    def fake_scent(real: str) -> str:
        return '7' + real[1:] if real else real

    @staticmethod
    def fake_empatica_short(real: str) -> str:
        return 'F' + real

    def obfuscate_key(self, key: str) -> str:
        m = self.KEY_VISITOR.match(key)
        if m:
            tid = m.group(1)
            if self.UUID_RE.match(tid) or self.AT_ID_RE.match(tid):
                return f'Visitors:{self.fake_ticket(tid)}{m.group(2) or ""}'
        m = self.KEY_WATCH.match(key)
        if m:
            eid = m.group(1)
            if self.EMPATICA_RE.match(eid):
                return f'Wearables:WatchDevices:{self.fake_empatica(eid)}{m.group(2) or ""}'
        m = self.KEY_SCENT.match(key)
        if m:
            sid = m.group(1)
            if self.SCENT_RE.match(sid):
                return f'Wearables:Scent:{self.fake_scent(sid)}{m.group(2) or ""}'
        return key

    def obfuscate_value(self, key: str, value_bytes: bytes) -> bytes:
        try:
            decoded = msgpack.unpackb(value_bytes, raw=False)
        except Exception:
            return value_bytes
        if key.endswith(':EmpaticaDeviceIDShort') and isinstance(decoded, str):
            new: Any = self.fake_empatica_short(decoded)
        else:
            new = self._walk(decoded, scent_context='Scent' in key)
        try:
            return msgpack.packb(new, use_bin_type=True)
        except Exception:
            return value_bytes

    def _walk(self, val: Any, scent_context: bool) -> Any:
        if isinstance(val, dict):
            return {self._walk(k, scent_context): self._walk(v, scent_context)
                    for k, v in val.items()}
        if isinstance(val, list):
            return [self._walk(v, scent_context) for v in val]
        if isinstance(val, str):
            return self._sub(val, scent_context)
        if isinstance(val, bytes):
            try:
                s = val.decode('utf-8')
            except UnicodeDecodeError:
                return val
            new = self._sub(s, scent_context)
            return new.encode('utf-8') if new != s else val
        return val

    def _sub(self, s: str, scent_context: bool) -> str:
        pattern = self.EMBEDDED_WITH_SCENT if scent_context else self.EMBEDDED

        def repl(m: re.Match[str]) -> str:
            text = m.group(0)
            groups = m.groupdict()
            if groups.get('at') or groups.get('uuid'):
                return self.fake_ticket(text)
            if groups.get('empatica'):
                return self.fake_empatica(text)
            if groups.get('scent'):
                return self.fake_scent(text)
            return text

        return pattern.sub(repl, s)
