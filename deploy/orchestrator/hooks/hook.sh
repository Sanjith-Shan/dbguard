#!/bin/sh
# Orchestrator recovery hook for the DBGuard lab baseline.
#   hook.sh <phase> <cluster alias> <failed host> <successor host> <failure type>
# Phases
#   detect                OnFailureDetectionProcesses, records the detection time
#   pre-failover          PreFailoverProcesses, fences the failed primary through its agent
#                         (POST :8080/fence, 3 s max) so HAProxy stops routing to it
#   post-master-failover  PostMasterFailoverProcesses, POST :8080/promote on the successor
#                         so the agent flips semi-sync to the source side and clears its fence
#                         flag. Orchestrator already repointed the other replicas and cleared
#                         read_only itself (ApplyMySQLPromotionAfterMasterFailover).
# Every call appends one JSON line (Event-row shaped where it can be) to
# $DBGUARD_HOOKS_DIR/events.jsonl, which the chaos harness reads.
set -u
phase="$1"; rs="$2"; failed="$3"; successor="$4"; ftype="$5"
dir="${DBGUARD_HOOKS_DIR:-/var/lib/orchestrator/hooks}"
mkdir -p "$dir"
out="$dir/events.jsonl"
now() { date +%s.%N | cut -c1-17; }
http_post() {  # url -> "code body" (curl if present, else wget)
  if command -v curl >/dev/null 2>&1; then
    curl -s -m 3 -X POST -H 'Content-Type: application/json' -d '{}' -w ' %{http_code}' "$1" 2>/dev/null
  else
    wget -q -T 3 -O - --post-data='{}' --header='Content-Type: application/json' "$1" 2>/dev/null && echo ' 200'
  fi
}
sub() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", a-b}'; }
esc() { printf '%s' "$1" | tr -d '\n' | sed 's/\\/\\\\/g; s/"/\\"/g'; }
ts=$(now)
case "$phase" in
  detect)
    echo "$ts" > "$dir/detect.$rs"
    printf '{"ts":%s,"rs":"%s","type":"suspect","mode":"orchestrator","old_primary":"%s","new_primary":null,"trigger":"%s","hook":"%s","note":null}\n' \
      "$ts" "$rs" "$failed" "$ftype" "$phase" >> "$out"
    ;;
  pre-failover|pre-graceful)
    r=$(http_post "http://$failed:8080/fence"); t2=$(now)
    echo "$ts $t2 $r" > "$dir/fence.$rs"
    printf '{"ts":%s,"rs":"%s","type":"fence","mode":"orchestrator","old_primary":"%s","new_primary":null,"trigger":"%s","hook":"%s","fence_s":%s,"note":"%s"}\n' \
      "$ts" "$rs" "$failed" "$ftype" "$phase" "$(sub $t2 $ts)" "$(esc "$r")" >> "$out"
    ;;
  post-master-failover|post-graceful)
    r=$(http_post "http://$successor:8080/promote"); t2=$(now)
    det=$(cat "$dir/detect.$rs" 2>/dev/null || echo "$ts")
    typ=failover; [ "$phase" = post-graceful ] && typ=switchover
    printf '{"ts":%s,"rs":"%s","type":"%s","mode":"orchestrator","old_primary":"%s","new_primary":"%s","trigger":"%s","hook":"%s","detect_ts":%s,"total_s":%s,"steps":{"promote":{"hook_s":%s}},"rejoin":null,"note":"%s"}\n' \
      "$t2" "$rs" "$typ" "$failed" "$successor" "$ftype" "$phase" "$det" \
      "$(sub $t2 $det)" "$(sub $t2 $ts)" "$(esc "$r")" >> "$out"
    ;;
  *)
    printf '{"ts":%s,"rs":"%s","type":"%s","mode":"orchestrator","old_primary":"%s","new_primary":"%s","trigger":"%s","hook":"%s","note":null}\n' \
      "$ts" "$rs" "$phase" "$failed" "$successor" "$ftype" "$phase" >> "$out"
    ;;
esac
exit 0
