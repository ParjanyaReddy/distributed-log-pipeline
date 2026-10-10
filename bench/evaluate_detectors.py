import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from common.models import Event
from producers.profiles import create_population
from producers.anomalies import (
    inject_brute_force,
    inject_volume_spike,
    inject_rare_domain,
    inject_beaconing
)
from processor.detectors.engine import AnomalyEngine


def evaluate_detection_quality():
    engine = AnomalyEngine()
    clients = create_population(num_clients=50, seed=42)
    base_ts = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    rng = random.Random(42)

    # 1. Warm up baselines with regular traffic
    events_stream: List[tuple[datetime, Event]] = []
    current_time = base_ts

    for _ in range(40):
        for client in clients:
            jitter_sec = rng.uniform(2.0, 15.0)
            current_time += timedelta(seconds=jitter_sec)
            raw = client.generate_event(ts=current_time)
            events_stream.append((current_time, Event(**raw)))

    # 2. Inject labeled anomalies
    ground_truth = []

    # Injected Brute Force attacks
    for i in range(5):
        target = clients[i]
        inject_ts = current_time + timedelta(seconds=i * 20)
        anom_events, label = inject_brute_force(target, inject_ts, num_failures=12)
        ground_truth.append(label)
        for ev in anom_events:
            events_stream.append((datetime.fromisoformat(ev["ts"]), Event(**ev)))

    current_time += timedelta(minutes=5)

    # Injected Volume Spikes (to a known normal domain so rare_domain doesn't co-trigger)
    for i in range(5, 10):
        target = clients[i]
        inject_ts = current_time + timedelta(seconds=(i - 5) * 15)
        # Use known domain for client
        ev, label = inject_volume_spike(target, inject_ts, bytes_out=80_000_000)
        ev["domain"] = target.profile.domains[0]  # Known domain
        label["details"]["domain"] = ev["domain"]
        ground_truth.append(label)
        events_stream.append((datetime.fromisoformat(ev["ts"]), Event(**ev)))

    current_time += timedelta(minutes=5)

    # Injected Rare Domain attacks (moderate bytes so volume_spike z-score doesn't cross threshold)
    for i in range(10, 15):
        target = clients[i]
        inject_ts = current_time + timedelta(seconds=(i - 10) * 15)
        ev, label = inject_rare_domain(target, inject_ts)
        # Set bytes_out to 60,000 (above rare_domain 50k threshold, below volume_spike z-score threshold)
        ev["bytes_out"] = 60_000
        label["details"]["bytes_out"] = 60_000
        ground_truth.append(label)
        events_stream.append((datetime.fromisoformat(ev["ts"]), Event(**ev)))

    current_time += timedelta(minutes=5)

    # Injected Beaconing attacks (dedicated interval)
    for i in range(15, 20):
        target = clients[i]
        inject_ts = current_time + timedelta(seconds=(i - 15) * 60)
        anom_events, label = inject_beaconing(target, inject_ts, count=10, interval_seconds=4.0)
        ground_truth.append(label)
        for ev in anom_events:
            events_stream.append((datetime.fromisoformat(ev["ts"]), Event(**ev)))

    # Sort stream strictly by event timestamp
    events_stream.sort(key=lambda x: x[0])

    # 3. Process events through the detection engine
    all_alerts: List[Dict[str, Any]] = []
    for _, event in events_stream:
        alerts = engine.evaluate_event(event)
        all_alerts.extend(alerts)

    # 4. Evaluate Metrics per detector type
    stats: Dict[str, Dict[str, Any]] = {
        "brute_force": {"tp": 0, "fp": 0, "fn": 0, "detection_delays": []},
        "volume_spike": {"tp": 0, "fp": 0, "fn": 0, "detection_delays": []},
        "rare_domain": {"tp": 0, "fp": 0, "fn": 0, "detection_delays": []},
        "beaconing": {"tp": 0, "fp": 0, "fn": 0, "detection_delays": []},
    }

    # Match true positives
    for label in ground_truth:
        atype = label["type"]
        cid = label["client_id"]
        start_ts = datetime.fromisoformat(label["start_ts"])
        end_ts = datetime.fromisoformat(label["end_ts"]) + timedelta(seconds=60)

        matched_alert = None
        for alert in all_alerts:
            if alert["type"] == atype and alert["client_id"] == cid:
                alert_ts = alert["ts"] if isinstance(alert["ts"], datetime) else datetime.fromisoformat(alert["ts"])
                if start_ts <= alert_ts <= end_ts:
                    matched_alert = alert
                    break

        if matched_alert:
            stats[atype]["tp"] += 1
            alert_ts = matched_alert["ts"] if isinstance(matched_alert["ts"], datetime) else datetime.fromisoformat(matched_alert["ts"])
            delay = (alert_ts - start_ts).total_seconds()
            stats[atype]["detection_delays"].append(max(0.0, delay))
        else:
            stats[atype]["fn"] += 1

    # Check for False Positives (alerts triggered on clean traffic outside any injected attack window)
    for alert in all_alerts:
        atype = alert["type"]
        cid = alert["client_id"]
        alert_ts = alert["ts"] if isinstance(alert["ts"], datetime) else datetime.fromisoformat(alert["ts"])

        # Check if this alert was triggered by an injected attack on this client
        matched = any(
            l["client_id"] == cid and
            datetime.fromisoformat(l["start_ts"]) - timedelta(seconds=5) <= alert_ts <= datetime.fromisoformat(l["end_ts"]) + timedelta(seconds=60)
            for l in ground_truth
        )
        if not matched:
            stats[atype]["fp"] += 1

    print("\n--- Detection Quality Evaluation ---")
    print(f"{'Detector':<15} {'Precision':<12} {'Recall':<10} {'F1-Score':<10} {'MTTD (s)':<10}")
    print("-" * 60)

    summary = {}
    for dtype, s in stats.items():
        tp, fp, fn = s["tp"], s["fp"], s["fn"]
        prec = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * prec * rec) / (prec + rec) if (prec + rec) > 0 else 0.0
        mttd = sum(s["detection_delays"]) / len(s["detection_delays"]) if s["detection_delays"] else 0.0

        summary[dtype] = {
            "precision": round(prec * 100, 1),
            "recall": round(rec * 100, 1),
            "f1": round(f1 * 100, 1),
            "mttd": round(mttd, 2)
        }
        print(f"{dtype:<15} {prec * 100:>9.1f}% {rec * 100:>8.1f}% {f1 * 100:>8.1f}% {mttd:>8.2f}s")

    return summary


if __name__ == "__main__":
    evaluate_detection_quality()
