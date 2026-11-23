#!/usr/bin/env python3
"""Regenerates src/provider_capabilities.json from the pinned reference.

Each provider's capabilities are the reference adapter's
`... satisfies OrchestrationV2ProviderCapabilities` object (CodexAdapterV2.ts,
ClaudeAdapterV2.ts), a plain literal Node evaluates.

Usage: scripts/update_provider_capabilities.py <reference checkout> [--check]
"""
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "src/provider_capabilities.json"
ADAPTERS = {"claudeAgent": "ClaudeAdapterV2.ts", "codex": "CodexAdapterV2.ts"}


def capabilities(checkout: pathlib.Path) -> dict:
    out = {}
    for driver, name in ADAPTERS.items():
        source = (checkout / "apps/server/src/orchestration-v2/Adapters" / name).read_text()
        end = source.index("} satisfies OrchestrationV2ProviderCapabilities;")
        start = source.rindex(" = {", 0, end)
        literal = source[start + 3:end + 1]
        out[driver] = json.loads(subprocess.run(
            ["node", "-e", f"process.stdout.write(JSON.stringify(({literal})))"],
            check=True, capture_output=True, text=True).stdout)
    return out


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    text = json.dumps(capabilities(pathlib.Path(sys.argv[1])), indent=2, sort_keys=True) + "\n"
    if "--check" in sys.argv[2:]:
        if OUTPUT.read_text() != text:
            sys.exit(f"{OUTPUT.relative_to(ROOT)} is not the pinned reference's; run {pathlib.Path(__file__).name}")
        return
    OUTPUT.write_text(text)


if __name__ == "__main__":
    main()
