"""Capture malformed settings RPC responses while asserting the input file stays untouched."""

import json


def malformed_settings(ws, state, frame):
    path = state / "settings.json"
    cases = {
        "empty": b"",
        "truncated-object": b"{",
        "truncated-string": b'{"worktreesDirectory":"/tmp',
        "invalid-key": b'{"defaultAutoPull":true, bad: false}',
        "invalid-value": b'{"defaultAutoPull":trux}',
        "invalid-array": b'{"hiddenThreadTags":["good" "bad"]}',
        "bad-escape": rb'{"defaultTheme":"\q"}',
        "double-comma": b'{"defaultAutoPull":true,,}',
        "unclosed-comment": b"{/* no closing delimiter",
        "mixed-line-endings": b'{\r/* multi\nline */ // comment\r\n"defaultAutoPull":}',
        "extra-text": b'{"defaultAutoPull":true} settings',
        "invalid-utf8-token": b'{"defaultAutoPull":\xff}',
    }
    for label, contents in cases.items():
        path.write_bytes(contents)
        packet = frame(
            ws, label, "server.updateSettings", {"patch": {"defaultAutoPull": False}}
        )
        exit = json.loads(packet)["exit"]
        assert exit["_tag"] == "Failure", label
        err = exit["cause"][0]["error"]
        assert err["operation"] == "read-file" and err["cause"] == {
            "name": "SchemaError",
            "message": "Expected a valid JSON string",
        }, (label, err)
        assert path.read_bytes() == contents, label
        yield packet
    for label, contents, expected in (
        ("utf8-bom", b'\xef\xbb\xbf{"worktreesDirectory":"/with-bom"}', "/with-bom"),
        (
            "replacement-in-string",
            b'{"worktreesDirectory":"bad\xffchar"}',
            "bad\ufffdchar",
        ),
    ):
        path.write_bytes(contents)
        packet = frame(
            ws, label, "server.updateSettings", {"patch": {"defaultAutoPull": False}}
        )
        assert json.loads(packet)["exit"]["value"]["worktreesDirectory"] == expected, (
            label
        )
        yield packet
        yield path.read_bytes()
