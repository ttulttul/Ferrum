"""Known differences for the parity suites that have no expected-failure file
of their own (client_thread, client_rpc, shared_store and provider_status
keep theirs).

scripts/parity_gaps.json maps a suite (its script's name without `.py`) to
its known differences: check name -> reason. A listed check that differs is
reported as XFAIL and does not fail the suite. One that matches is XPASS and
fails the suite until it is removed, unless its reason starts with "Racy:" (a
race either server can win, so the check matches on some runs only). Each
reason names the upstream change behind the difference.

    gaps = Gaps("parity_harness")
    if differs:
        failures += gaps.differs(name, ["original ...", "rust     ..."])
    ...
    failures += gaps.finish()
"""
import json
from pathlib import Path

RACY = "Racy:"
FILE = Path(__file__).resolve().parent / "parity_gaps.json"


class Gaps:
    """The known differences of one suite, and what this run saw of them."""

    def __init__(self, suite):
        self.suite = suite
        self.known = json.loads(FILE.read_text()).get(suite, {})
        self.differed = set()

    def differs(self, check, lines=()):
        """Report that `check` differs; 1 when that fails the suite, else 0."""
        self.differed.add(check)
        if check in self.known:
            print(f"XFAIL {check}: {self.known[check]}", flush=True)
            return 0
        print(f"FAIL {check}", flush=True)
        for line in lines:
            print(f"  {line}", flush=True)
        return 1

    def expected(self, check):
        """Whether `check` is a known difference (for suites that must skip
        what follows a known difference)."""
        return check in self.known

    def finish(self, ran=None):
        """Report the known differences that matched; the number that fail
        the suite. `ran` limits the report to the checks this run reached."""
        failures = 0
        for check, reason in sorted(self.known.items()):
            if check in self.differed or (ran is not None and check not in ran):
                continue
            if reason.startswith(RACY):
                print(f"RACY {check}: matched this time", flush=True)
            else:
                failures += 1
                print(f"XPASS {check}: remove it from parity_gaps.json ({self.suite})", flush=True)
        return failures
