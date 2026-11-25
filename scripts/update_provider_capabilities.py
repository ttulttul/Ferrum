#!/usr/bin/env python3
"""Regenerates src/provider_capabilities.json from the pinned reference.

Each provider's capabilities are the reference adapter's
`... satisfies OrchestrationV2ProviderCapabilities` object (CodexAdapterV2.ts,
ClaudeAdapterV2.ts, provider-pi's adapter.ts, provider-opencode's 1.x and OpenCode 2
adapters), a plain literal Node evaluates; Grok's
(provider-grok's adapter.ts) spreads the generic ACP adapter's
`AcpProviderCapabilitiesV2`, which Node imports from the reference to
evaluate it. They are what a Grok session negotiates with an agent that
can load sessions and not fork them, as Grok can. ACP Registry agents run
the generic adapter, whose `AcpProviderCapabilitiesV2` is taken as it is.

Usage: scripts/update_provider_capabilities.py <reference checkout> [--check]
"""
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "src/provider_capabilities.json"
ADAPTERS = {"antigravity": "apps/server/src/orchestration-v2/Adapters/AntigravityAdapterV2.ts",
            "claudeAgent": "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts",
            "codex": "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts",
            "grok": "packages/provider-grok/src/server/adapter.ts",
            "kiro": "packages/provider-kiro/src/server/adapter.ts",
            "opencode": "packages/provider-opencode/src/server/adapter.ts",
            # OpenCode 2's adapter, whose capabilities an OpenCode 2 thread's session reports.
            "opencode2": "packages/provider-opencode/src/server/v2/adapter.ts",
            "pi": "packages/provider-pi/src/server/adapter.ts"}
# Drivers whose capabilities are an exported constant as it is (ACP Registry agents run the generic
# ACP adapter's): the constant, and the reference module that exports it.
CONSTANTS = {"acpRegistry": ("AcpProviderCapabilitiesV2", "packages/provider-acp/src/server/adapter.ts")}
# What a literal may spread, and the reference module that exports it.
IMPORTS = {"AcpProviderCapabilitiesV2": "packages/provider-acp/src/server/adapter.ts"}


def capabilities(checkout: pathlib.Path) -> dict:
    out = {}
    for driver, path in ADAPTERS.items():
        source = (checkout / path).read_text()
        end = source.index("} satisfies OrchestrationV2ProviderCapabilities;")
        start = source.rindex(" = {", 0, end)
        literal = source[start + 3:end + 1]
        imports = "".join(
            f"const {{ {name} }} = await import({json.dumps((checkout / module).resolve().as_uri())});"
            for name, module in IMPORTS.items() if name in literal)
        out[driver] = json.loads(subprocess.run(
            ["node", "--input-type=module", "-e",
             f"{imports}process.stdout.write(JSON.stringify(({literal})))"],
            check=True, capture_output=True, text=True, cwd=checkout).stdout)
    for driver, (name, module) in CONSTANTS.items():
        url = json.dumps((checkout / module).resolve().as_uri())
        out[driver] = json.loads(subprocess.run(
            ["node", "--input-type=module", "-e",
             f"const {{ {name} }} = await import({url});process.stdout.write(JSON.stringify({name}))"],
            check=True, capture_output=True, text=True, cwd=checkout).stdout)
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
