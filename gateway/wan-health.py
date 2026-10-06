#!/usr/bin/python3
"""Forced SSH command: emit only current WAN health, never execute client input."""
import json
import subprocess
import time


def read(path):
    result = subprocess.run(
        ["/usr/bin/ubios-udapi-client", "-r", "GET", path],
        check=True, capture_output=True, timeout=3,
    )
    return json.loads(result.stdout)


services = read("/services")["wanFailover"]
links = read("/interfaces")
print(json.dumps({
    "sampled_at": int(time.time()),
    "enabled": services["enabled"],
    "groups": [{"interfaces": g["interfaces"], "metric": g["metric"],
                "active": g["status"]["active"], "health": g["status"]["health"]}
               for g in services["failoverGroups"]],
    "wans": [{"interface": w["interface"], "metric": w["metric"],
              "health": w["status"]["health"]}
             for w in services["wanInterfaces"]],
    "links": [{"interface": link["identification"]["id"],
               "enabled": link["status"]["enabled"],
               "current_enabled": link["status"]["currentEnabled"],
               "plugged": link["status"]["plugged"],
               "wan_status": link["status"].get("wanStatus"),
               "timestamp_ms": link["status"]["timestamp"]}
              for link in links if link["identification"]["id"] in
              {w["interface"] for w in services["wanInterfaces"]}],
}, separators=(",", ":")))
