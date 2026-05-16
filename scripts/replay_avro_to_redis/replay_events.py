#!/usr/bin/env python3
"""
Script to replay Avro-encoded Redis events back to a Redis server.
Reads events from an Avro file and executes corresponding Redis commands.
"""

import sys
import time
from pathlib import Path
from typing import Any, cast

import fastavro
import msgpack
import redis

from obfuscator import Obfuscator
from filter import KeyFilter


def replay_events(
    avro_file: Path,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    redis_password: str | None = None,
    dry_run: bool = False,
    realtime: bool = True,
    speed: float = 1.0,
    verbose: bool = False,
    loop: int = 1,
    channel_filter: KeyFilter | None = None,
    key_filter: KeyFilter | None = None,
    obfuscate: bool = False,
) -> None:
    """
    Read events from Avro file and replay them to Redis.

    Args:
        avro_file: Path to the .avro file
        redis_host: Redis server hostname
        redis_port: Redis server port
        redis_db: Redis database number
        redis_password: Redis password (optional)
        dry_run: If True, print commands without executing them
        realtime: If True, replay events with original timing (default: True)
        speed: Playback speed multiplier (1.0 = realtime, 2.0 = 2x speed, 0.5 = half speed)
        verbose: If True, print commands to console (default: False)
        loop: Number of times to replay (0 = infinite)
        channel_filter: Optional allowlist; channels not matching are skipped
        key_filter: Optional allowlist; keys not matching are skipped
        obfuscate: If True, replace ticket/device IDs with non-overlapping fakes
    """
    # Connect to Redis (unless dry run)
    r: redis.Redis | None  # type: ignore[type-arg]
    if not dry_run:
        r = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=False
        )
        try:
            r.ping()
            auth_info = " (authenticated)" if redis_password else ""
            print(
                f"✓ Connected to Redis at {redis_host}:{redis_port} (db={redis_db}){auth_info}")
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"DRY RUN MODE - commands will be printed but not executed")
        r = None

    obfuscator = Obfuscator() if obfuscate else None
    if obfuscator is not None:
        print("✓ Obfuscation enabled — ticket/device IDs will be replaced with fakes")

    # Read and replay events
    total_event_count = 0
    total_channel_count = 0
    total_key_count = 0
    total_filtered_channel_count = 0
    total_filtered_key_count = 0
    iteration = 0
    infinite = loop == 0

    try:
        while infinite or iteration < loop:
            iteration += 1
            if loop != 1:
                label = "∞" if infinite else f"{loop}"
                print(f"\n▶ Loop iteration {iteration}/{label}")

            event_count = 0
            channel_count = 0
            key_count = 0
            filtered_channel_count = 0
            filtered_key_count = 0

            first_timestamp: int | None = None
            start_time: float | None = None

            with open(avro_file, 'rb') as f:
                reader = fastavro.reader(f)
                for record in reader:
                    # Cast to dict to satisfy type checker
                    rec = cast(dict[str, Any], record)

                    event_type: str = rec['type']
                    key: str = rec['key']
                    value: bytes = rec['value']
                    ttl: int | None = rec['ttl']
                    # milliseconds since epoch
                    timestamp: int = rec['timestamp']

                    # Handle timing for realtime playback
                    if realtime:
                        if first_timestamp is None:
                            # First event - initialize timing
                            first_timestamp = timestamp
                            start_time = time.time()
                        else:
                            # Calculate when this event should be played relative to first event
                            assert start_time is not None  # Type narrowing
                            elapsed_ms = timestamp - first_timestamp
                            elapsed_s = elapsed_ms / 1000.0
                            adjusted_elapsed_s = elapsed_s / speed

                            # Calculate target time and sleep if needed
                            target_time = start_time + adjusted_elapsed_s
                            current_time = time.time()
                            sleep_time = target_time - current_time

                            if sleep_time > 0:
                                time.sleep(sleep_time)
                            elif sleep_time < -1.0:
                                # We're falling behind by more than 1 second
                                print(
                                    f"\n⚠ Warning: Playback is {-sleep_time:.2f}s behind schedule", file=sys.stderr)

                    if event_type == 'channel':
                        if channel_filter is not None and not channel_filter.matches(key):
                            filtered_channel_count += 1
                        else:
                            if obfuscator is not None:
                                value = obfuscator.obfuscate_value(key, value)
                                key = obfuscator.obfuscate_key(key)
                            # Publish to channel
                            if dry_run or verbose:
                                try:
                                    decoded_value = msgpack.unpackb(
                                        value, raw=False)
                                    print(f"PUBLISH {key} {decoded_value}")
                                except Exception:
                                    print(
                                        f"PUBLISH {key} <{len(value)} bytes (decode failed)>")
                            if not dry_run:
                                assert r is not None  # Type narrowing for mypy
                                r.publish(key, value)
                            channel_count += 1

                    elif event_type == 'key':
                        if key_filter is not None and not key_filter.matches(key):
                            filtered_key_count += 1
                        else:
                            if obfuscator is not None:
                                value = obfuscator.obfuscate_value(key, value)
                                key = obfuscator.obfuscate_key(key)
                            # Set key with optional TTL
                            if dry_run or verbose:
                                try:
                                    decoded_value = msgpack.unpackb(
                                        value, raw=False)
                                    ttl_info = f" EX {ttl}" if ttl is not None else ""
                                    print(
                                        f"SET {key}{ttl_info} {decoded_value}")
                                except Exception:
                                    ttl_info = f" (TTL: {ttl}s)" if ttl is not None else ""
                                    print(
                                        f"SET {key} <{len(value)} bytes (decode failed)>{ttl_info}")
                            if not dry_run:
                                assert r is not None  # Type narrowing for mypy
                                if ttl is not None and ttl > 0:
                                    r.setex(key, ttl, value)
                                else:
                                    r.set(key, value)
                            key_count += 1

                    event_count += 1

                    if event_count % 100 == 0:
                        print(f"Processed {event_count} events...", end='\r')

            total_event_count += event_count
            total_channel_count += channel_count
            total_key_count += key_count
            total_filtered_channel_count += filtered_channel_count
            total_filtered_key_count += filtered_key_count

            filter_summary = ""
            if filtered_channel_count or filtered_key_count:
                filter_summary = (f", filtered {filtered_channel_count} channels"
                                  f" / {filtered_key_count} keys")
            print(f"\n✓ Iteration {iteration} complete: {event_count} events "
                  f"({channel_count} publishes, {key_count} key sets{filter_summary})")

    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")

    print(
        f"\n✓ Replayed {total_event_count} events across {iteration} iteration(s):")
    channel_suffix = f" ({total_filtered_channel_count} filtered)" if channel_filter else ""
    key_suffix = f" ({total_filtered_key_count} filtered)" if key_filter else ""
    print(f"  - {total_channel_count} channel publishes{channel_suffix}")
    print(f"  - {total_key_count} key sets{key_suffix}")
