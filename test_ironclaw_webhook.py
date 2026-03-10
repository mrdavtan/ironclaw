#!/usr/bin/env python3
"""
test_ironclaw_webhook.py — End-to-end smoke test for IronClaw webhook.

Tests the full chain:
  gateway → IronClaw /webhook → LLM (via codex-bridge) → response

Prerequisites:
  1. codex-bridge.py running on :9100
  2. IronClaw running with HTTP_PORT=8080

Usage:
    python3 tools/ironclaw/test_ironclaw_webhook.py
    python3 tools/ironclaw/test_ironclaw_webhook.py --host 127.0.0.1 --port 8080
"""

import json
import sys
import time
import urllib.request
import urllib.error


def check_health(base_url: str) -> bool:
    """Check IronClaw health endpoint."""
    try:
        req = urllib.request.Request(f"{base_url}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return data.get("status") == "healthy"
    except Exception:
        return False


def check_bridge(bridge_url: str = "http://127.0.0.1:9100/health") -> bool:
    """Check Codex bridge health."""
    try:
        req = urllib.request.Request(bridge_url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return "data" in data or "uptime" in data
    except Exception:
        return False


def send_webhook(
    base_url: str, content: str, secret: str, wait: bool = True, timeout: int = 120
) -> dict:
    """Send a webhook request to IronClaw."""
    payload = {
        "content": content,
        "secret": secret,
        "wait_for_response": wait,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/webhook",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IronClaw webhook smoke test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--secret", default="doc2db-ironclaw-dev")
    parser.add_argument("--skip-bridge-check", action="store_true")
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    results = []

    print("=" * 60)
    print("IronClaw Webhook Smoke Test")
    print("=" * 60)
    print(f"Target: {base_url}")
    print()

    # Test 1: Health check
    print("[1/5] Health check...", end=" ", flush=True)
    healthy = check_health(base_url)
    results.append(("Health check", healthy))
    print("✅ healthy" if healthy else "❌ FAILED")
    if not healthy:
        print("\n  IronClaw not reachable. Start it:")
        print("  cd tools/ironclaw && ./target/release/ironclaw &")
        print("\n  Aborting remaining tests.")
        sys.exit(1)

    # Test 2: Codex bridge check
    if not args.skip_bridge_check:
        print("[2/5] Codex bridge check...", end=" ", flush=True)
        bridge_ok = check_bridge()
        results.append(("Codex bridge", bridge_ok))
        print("✅ running" if bridge_ok else "⚠️  not running (LLM tests may fail)")
    else:
        print("[2/5] Codex bridge check... SKIPPED")
        results.append(("Codex bridge", None))

    # Test 3: Webhook auth — wrong secret should fail
    print("[3/5] Auth rejection (wrong secret)...", end=" ", flush=True)
    try:
        send_webhook(base_url, "test", "wrong-secret", wait=False, timeout=10)
        results.append(("Auth rejection", False))
        print("❌ FAILED (accepted wrong secret)")
    except urllib.error.HTTPError as e:
        if e.code == 401:
            results.append(("Auth rejection", True))
            print("✅ rejected (401)")
        else:
            results.append(("Auth rejection", False))
            print(f"❌ FAILED (HTTP {e.code}, expected 401)")
    except Exception as e:
        results.append(("Auth rejection", False))
        print(f"❌ FAILED ({e})")

    # Test 4: Webhook fire-and-forget (no wait)
    print("[4/5] Fire-and-forget message...", end=" ", flush=True)
    try:
        resp = send_webhook(base_url, "ping", args.secret, wait=False, timeout=10)
        has_id = "message_id" in resp
        results.append(("Fire-and-forget", has_id))
        print(f"✅ accepted (id={resp.get('message_id', '?')[:8]}...)" if has_id else "❌ FAILED")
    except Exception as e:
        results.append(("Fire-and-forget", False))
        print(f"❌ FAILED ({e})")

    # Test 5: Synchronous LLM round-trip
    print("[5/5] Sync LLM round-trip (wait_for_response=true)...", end=" ", flush=True)
    try:
        t0 = time.time()
        resp = send_webhook(
            base_url,
            "Reply with exactly: IRONCLAW_WEBHOOK_OK",
            args.secret,
            wait=True,
            timeout=120,
        )
        elapsed = time.time() - t0
        reply = resp.get("response")
        has_reply = reply is not None and len(reply) > 0
        results.append(("Sync LLM round-trip", has_reply))
        if has_reply:
            print(f"✅ got reply ({len(reply)} chars, {elapsed:.1f}s)")
            print(f"     Response: {reply[:100]}{'...' if len(reply) > 100 else ''}")
        else:
            print(f"❌ FAILED (no response field, got: {list(resp.keys())})")
    except urllib.error.HTTPError as e:
        results.append(("Sync LLM round-trip", False))
        body = e.read().decode("utf-8", errors="replace")[:200]
        print(f"❌ FAILED (HTTP {e.code}: {body})")
    except Exception as e:
        results.append(("Sync LLM round-trip", False))
        print(f"❌ FAILED ({e})")

    # Summary
    print()
    print("=" * 60)
    passed = sum(1 for _, ok in results if ok is True)
    failed = sum(1 for _, ok in results if ok is False)
    skipped = sum(1 for _, ok in results if ok is None)
    total = len(results)
    print(f"Results: {passed}/{total} passed, {failed} failed, {skipped} skipped")

    # Connector compatibility check
    if any(name == "Sync LLM round-trip" and ok for name, ok in results):
        print()
        print("✅ CONNECTOR COMPATIBLE: IronClaw returns 'response' field")
        print("   → agent_connector.py will extract reply text with zero changes")
    print("=" * 60)

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
