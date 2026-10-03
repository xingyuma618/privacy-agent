#!/usr/bin/env python3
"""Route only the agent role of formal repetition 2 to approved local replicas."""
from __future__ import annotations

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

import runner


REPLICA_ENDPOINTS = {
    "sft": "http://127.0.0.1:18346/v1",
    "rl": "http://127.0.0.1:18347/v1",
}


def verify_replica(endpoint: str, served_model: str, checkpoint_path: str) -> None:
    with urllib.request.urlopen(endpoint.rstrip("/") + "/models", timeout=10) as response:
        ids = [str(row["id"]) for row in json.load(response).get("data", [])]
    if served_model not in ids:
        raise RuntimeError(f"replica expected {served_model!r}, observed {ids!r}")
    expected_path = str(Path(checkpoint_path).resolve())
    port = endpoint.split(":")[2].split("/")[0]
    matching_commands = []
    for cmdline_path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            command = cmdline_path.read_bytes().replace(b"\0", b" ").decode(errors="replace")
        except (OSError, PermissionError):
            continue
        if "lmdeploy" in command and f"--server-port {port}" in command:
            matching_commands.append(command)
    if len(matching_commands) != 1:
        raise RuntimeError(f"expected one lmdeploy replica process for port {port}, found {len(matching_commands)}")
    command = matching_commands[0]
    if expected_path not in command or f"--model-name {served_model}" not in command:
        raise RuntimeError("replica process checkpoint path or served alias does not match the manifest")


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[1] not in REPLICA_ENDPOINTS or sys.argv[2] != "2":
        raise SystemExit("wrapper is restricted to: run_replica_eval.py {sft,rl} 2 [runner options]")
    arm = sys.argv[1]
    manifest = json.loads((runner.HERE / "manifest.json").read_text())
    logical_endpoint = manifest["models"][arm]["base_url"].rstrip("/")
    actual_endpoint = REPLICA_ENDPOINTS[arm].rstrip("/")
    checkpoint_path = manifest["models"][arm]["path"]
    served_model = manifest["models"][arm]["served_model_name"]
    verify_replica(actual_endpoint, served_model, checkpoint_path)

    wrapper_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    routing = {
        "logical_endpoint": logical_endpoint,
        "actual_endpoint": actual_endpoint,
        "checkpoint_path": checkpoint_path,
        "wrapper_sha256": wrapper_sha256,
        "verified_served_model": served_model,
        "serving_origin": "approved_local_replica_same_checkpoint",
    }

    original_verify = runner.verify_served_models
    original_factory = runner.traced_factory
    original_write_json = runner.write_json

    def replica_verify(expected):
        routed = dict(expected)
        _, model_name = routed["agent"]
        routed["agent"] = (actual_endpoint, model_name)
        return original_verify(routed)

    def replica_factory(real, endpoints):
        routed = dict(endpoints)
        routed["agent"] = actual_endpoint
        traced = original_factory(real, routed)

        def factory(*args, **kwargs):
            updated = dict(kwargs)
            if str(updated.get("base_url", "")).rstrip("/") == logical_endpoint:
                updated["base_url"] = actual_endpoint
            return traced(*args, **updated)

        return factory

    def routing_write_json(path, value):
        if path.name == "metadata.json" and isinstance(value, dict) and value.get("model") == arm:
            value = dict(value)
            value["agent_routing"] = routing
        return original_write_json(path, value)

    runner.verify_served_models = replica_verify
    runner.traced_factory = replica_factory
    runner.write_json = routing_write_json
    runner.main()


if __name__ == "__main__":
    main()
