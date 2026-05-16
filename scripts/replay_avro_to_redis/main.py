#!/usr/bin/env python3
"""
Script to replay Avro-encoded Redis events back to a Redis server.
Reads events from an Avro file and executes corresponding Redis commands.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Any, cast

import fastavro
import msgpack
import redis

from obfuscator import Obfuscator
from filter import KeyFilter, load_filter


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Replay Avro-encoded Redis events to a Redis server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Replay with original timing preserved (default)
  %(prog)s combined.avro
  
  # Replay at 2x speed with timing preserved
  %(prog)s combined.avro --speed 2.0
  
  # Replay as fast as possible (no timing delays)
  %(prog)s combined.avro --no-timing
  
  # Replay to remote Redis with authentication
  %(prog)s combined.avro --host redis.example.com --port 6380 --password mypassword
  
  # Dry run to see what would be executed
  %(prog)s combined.avro --dry-run
  
  # Loop infinitely
  %(prog)s combined.avro --loop inf
  
  # Loop 5 times
  %(prog)s combined.avro --loop 5

  # Replay only events matching allowlist patterns (one per line, * and ? wildcards)
  %(prog)s combined.avro --filter-channels channels.txt --filter-keys keys.txt

  # Obfuscate ticket and device IDs (uuid4 for tickets, F-prefix for Empatica, 7-prefix for Scent)
  %(prog)s combined.avro --obfuscate
        """
    )

    parser.add_argument('avro_file', type=Path, help='Path to the .avro file')
    parser.add_argument('--host', default='localhost',
                        help='Redis host (default: localhost)')
    parser.add_argument('--port', type=int, default=6379,
                        help='Redis port (default: 6379)')
    parser.add_argument('--db', type=int, default=0,
                        help='Redis database number (default: 0)')
    parser.add_argument('--password', type=str, default=None,
                        help='Redis password (optional)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print commands without executing')
    parser.add_argument('--no-timing', action='store_true',
                        help='Replay as fast as possible without timing delays')
    parser.add_argument('--speed', type=float, default=1.0,
                        help='Playback speed multiplier (default: 1.0)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print Redis commands to console')
    parser.add_argument('--loop', type=str, default='1',
                        help='Number of times to replay (default: 1, use "inf" for infinite)')
    parser.add_argument('--filter-channels', type=Path, default=None, metavar='PATH',
                        help='Allowlist file for channel publishes. One pattern per line; '
                             '"*" and "?" wildcards supported; "#" comments and blank lines skipped')
    parser.add_argument('--filter-keys', type=Path, default=None, metavar='PATH',
                        help='Allowlist file for key sets. One pattern per line; '
                             '"*" and "?" wildcards supported; "#" comments and blank lines skipped')
    parser.add_argument('--obfuscate', action='store_true',
                        help='Replace ticket/device IDs with non-overlapping fakes '
                             '(uuid4 for tickets, F-prefix for Empatica, 7-prefix for Scent)')

    args = parser.parse_args()

    if not args.avro_file.exists():
        print(f"✗ File not found: {args.avro_file}", file=sys.stderr)
        sys.exit(1)

    # Realtime is on by default, unless --no-timing is specified
    realtime = not args.no_timing

    if args.speed != 1.0 and args.no_timing:
        print("⚠ Warning: --speed has no effect with --no-timing", file=sys.stderr)

    # Parse loop value
    if args.loop.lower() == 'inf':
        loop_count = 0  # 0 means infinite
    else:
        try:
            loop_count = int(args.loop)
            if loop_count < 1:
                print("✗ --loop must be a positive integer or 'inf'",
                      file=sys.stderr)
                sys.exit(1)
        except ValueError:
            print(
                f"✗ Invalid --loop value: {args.loop!r} (use a number or 'inf')", file=sys.stderr)
            sys.exit(1)

    channel_filter: KeyFilter | None = None
    if args.filter_channels is not None:
        if not args.filter_channels.exists():
            print(
                f"✗ Channel filter file not found: {args.filter_channels}", file=sys.stderr)
            sys.exit(1)
        channel_filter = load_filter(args.filter_channels)
        print(
            f"✓ Loaded {len(channel_filter)} channel filter pattern(s) from {args.filter_channels}")

    key_filter: KeyFilter | None = None
    if args.filter_keys is not None:
        if not args.filter_keys.exists():
            print(
                f"✗ Key filter file not found: {args.filter_keys}", file=sys.stderr)
            sys.exit(1)
        key_filter = load_filter(args.filter_keys)
        print(
            f"✓ Loaded {len(key_filter)} key filter pattern(s) from {args.filter_keys}")

    replay_events(
        args.avro_file,
        args.host,
        args.port,
        args.db,
        args.password,
        args.dry_run,
        realtime,
        args.speed,
        args.verbose,
        loop_count,
        channel_filter,
        key_filter,
        args.obfuscate,
    )


if __name__ == '__main__':
    main()
