#!/usr/bin/env bash
# End-to-end check of the place-order flow against a running `docker compose up`.
# Needs: curl, python3. Exits non-zero if anything is wrong.
set -euo pipefail
ORDER=${ORDER_URL:-http://localhost:8000}
CID="demo-$(date +%s)"

wait_ready() {
  for _ in $(seq 1 60); do
    curl -fs "$ORDER/health" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "order service did not become ready" >&2; exit 1
}

field() { python3 -c "import sys,json; print(json.load(sys.stdin)['$1'])"; }

place() {  # $1 = amount, $2 = correlation id
  curl -fs -X POST "$ORDER/orders" \
    -H "Content-Type: application/json" \
    -H "X-Correlation-ID: $2" \
    -d "{\"customer_id\":\"c-1\",\"email\":\"demo@example.com\",\"items\":[{\"sku\":\"KETTLE-1\",\"qty\":1}],\"amount\":$1}"
}

wait_for_status() {  # $1 = order id, $2 = expected status
  for _ in $(seq 1 30); do
    s=$(curl -fs "$ORDER/orders/$1" | field status)
    [ "$s" = "$2" ] && return 0
    sleep 1
  done
  echo "order $1 never reached $2 (last: $s)" >&2; exit 1
}

wait_ready
echo "== Happy path: amount 100"
R=$(place 100 "$CID-ok"); echo "$R"
ID=$(echo "$R" | field order_id)
wait_for_status "$ID" CONFIRMED && echo "order $ID CONFIRMED"

echo "== Compensation: amount 5000 is declined"
R=$(place 5000 "$CID-fail"); echo "$R"
ID2=$(echo "$R" | field order_id)
wait_for_status "$ID2" CANCELLED && echo "order $ID2 CANCELLED, stock released"

echo "== Idempotent request: same key twice gives one order"
A=$(curl -fs -X POST "$ORDER/orders" -H "Content-Type: application/json" -H "Idempotency-Key: k-$CID" \
  -d '{"customer_id":"c-1","email":"d@example.com","items":[{"sku":"KETTLE-1","qty":1}],"amount":10}' | field order_id)
B=$(curl -fs -X POST "$ORDER/orders" -H "Content-Type: application/json" -H "Idempotency-Key: k-$CID" \
  -d '{"customer_id":"c-1","email":"d@example.com","items":[{"sku":"KETTLE-1","qty":1}],"amount":10}' | field order_id)
[ "$A" = "$B" ] && echo "same order returned: $A"

echo "SMOKE TEST PASSED"
