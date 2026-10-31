"""Black-box acceptance against a local running API and mock vendor (stdlib only)."""
import argparse
import copy
import datetime
import json
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def run(base_url, vendor_base_url):
    base_url = base_url.rstrip("/")
    vendor_base_url = vendor_base_url.rstrip("/")
    checks = []
    tasks = []
    run_id = uuid.uuid4().hex

    def call(method, path, body=None, expected=200, key=None):
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"} if data is not None else {}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            content = response.read(1 << 20)
            if response.code != expected:
                raise AssertionError(f"{method} {path}: {response.code}, expected {expected}: {content!r}")
            return json.loads(content) if "application/json" in response.headers.get("Content-Type", "") else content.decode()

    def wait(task_id, status):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            task = call("GET", f"/v1/notifications/{task_id}")
            if task["status"] == status:
                return task
            if task["status"] == "DEAD" and status != "DEAD":
                raise AssertionError(f"unexpected dead task: {task}")
            time.sleep(0.1)
        raise AssertionError(f"task {task_id} did not reach {status}: {task}")

    call("GET", "/healthz")
    originals = call("GET", "/v1/vendors")["items"]
    paths = {"ads": "/ads/conversions", "crm": "/crm/contacts", "inventory": "/inventory/adjustments"}
    examples = {
        "ads": {"user_id": "acceptance-user", "click_id": "acceptance-click"},
        "crm": {"contact_id": "acceptance-contact", "status": "paid"},
        "inventory": {"sku": "ACCEPTANCE-SKU", "delta": -2, "order_id": run_id},
    }
    configured = {}
    try:
        for old in originals:
            code = old["code"]
            if code not in paths:
                continue
            config = dict(old, endpoint_url=vendor_base_url + paths[code], max_attempts=3,
                          retry_base_seconds=1, retry_max_seconds=2, timeout_ms=2000)
            configured[code] = config
            call("PUT", f"/v1/vendors/{code}", config)
        if len(configured) != 3:
            raise AssertionError("schema.sql must seed ads, crm and inventory")
        for code in paths:
            body = {"source_system": "acceptance", "vendor_code": code,
                    "event_type": "acceptance.test", "payload": examples[code]}
            key = f"{run_id}:{code}"
            accepted = call("POST", "/v1/notifications", body, 202, key)
            task_id = accepted["task_id"]
            saved = wait(task_id, "SUCCEEDED")
            logs = call("GET", f"/v1/notifications/{task_id}/logs")["items"]
            assert logs[-1]["outcome"] == "SUCCESS" and saved["attempt_count"] == len(logs)
            duplicate = call("POST", "/v1/notifications", body, 202, key)
            assert duplicate["task_id"] == task_id and duplicate["duplicate"]
            conflict = copy.deepcopy(body)
            conflict["payload"]["different"] = True
            call("POST", "/v1/notifications", conflict, 409, key)
            tasks.append({"vendor": code, "task_id": task_id, "status": saved["status"],
                          "attempt_count": saved["attempt_count"]})
        checks.extend(["three vendor deliveries", "automatic retries", "duplicate submission", "idempotency conflict"])

        bad_config = dict(configured["inventory"], endpoint_url=vendor_base_url + "/missing")
        call("PUT", "/v1/vendors/inventory", bad_config)
        body = {"source_system": "acceptance", "vendor_code": "inventory",
                "event_type": "acceptance.repair", "payload": examples["inventory"]}
        failed = call("POST", "/v1/notifications", body, 202, f"{run_id}:repair")
        task_id = failed["task_id"]
        dead = wait(task_id, "DEAD")
        assert dead["attempt_count"] == 1
        call("PUT", "/v1/vendors/inventory", configured["inventory"])
        retried = call("POST", f"/v1/notifications/{task_id}/retry", expected=202)
        assert retried["task_id"] == task_id
        saved = wait(task_id, "SUCCEEDED")
        call("POST", f"/v1/notifications/{task_id}/retry", expected=409)
        logs = call("GET", f"/v1/notifications/{task_id}/logs")["items"]
        assert logs[0]["vendor_snapshot"]["endpoint_url"].endswith("/missing")
        assert logs[-1]["vendor_snapshot"]["endpoint_url"] == configured["inventory"]["endpoint_url"]
        assert logs[0]["outcome"] == "PERMANENT_FAILURE" and logs[-1]["outcome"] == "SUCCESS"
        assert saved["retry_start_attempt"] == 1
        tasks.append({"vendor": "inventory-repair", "task_id": task_id,
                      "status": saved["status"], "attempt_count": saved["attempt_count"]})
        page = call("GET", "/v1/notifications?source_system=acceptance&limit=100")
        assert {item["task_id"] for item in tasks}.issubset({item["task_id"] for item in page["items"]})
        metrics = call("GET", "/metrics")
        assert 'notification_tasks{status="SUCCEEDED"}' in metrics
        checks.extend(["failed task discovery", "configuration repair", "manual retry with stable task ID",
                       "historical configuration snapshots", "delivery log queries", "metrics"])
        return {"accepted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "base_url": base_url, "checks": checks, "tasks": tasks, "result": "PASS"}
    finally:
        for config in originals:
            if config["code"] in configured:
                call("PUT", "/v1/vendors/" + urllib.parse.quote(config["code"]), config)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--vendor-base-url", default="http://127.0.0.1:9090")
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("artifacts/acceptance.json"))
    args = parser.parse_args()
    result = run(args.base_url, args.vendor_base_url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
