"""
streaming_producer.py

SYNTHETIC real-time event producer.

Simulates trucks actively moving along REAL routes and emits events you
can feed into Structured Streaming. Two modes:

  1. FILE MODE (default, simplest for your time-boxed lab sessions):
     Writes small JSON files to a folder every N seconds, simulating a
     stream landing in cloud storage. Structured Streaming reads this
     folder with .readStream via Auto Loader / file source.
     -> This is the easiest way to demo "real-time" ingestion on
        Databricks Free Edition with zero extra infrastructure (no Kafka
        cluster needed).

  2. KAFKA MODE (optional, more "real", more setup):
     Sends events to a Kafka topic. Only use this if you set up a Kafka
     broker (e.g. Confluent Cloud free tier) -- not required for the
     core project to work or to impress in an interview; file-mode
     streaming via Auto Loader is a completely legitimate, commonly used
     real-world pattern on its own.

Run (file mode, default):
  python streaming_producer.py --mode file --output ./stream_output --duration-minutes 10

Each "tick" advances every active truck along its route by a realistic
distance (synthetic, but bounded by REAL speed limits from reference_data.py)
and emits:
  - 1 GPS event per truck
  - 1 sensor event per truck (engine temp, fuel; +cargo temp if refrigerated)
  - 0 or 1 delivery_event per truck (state machine: created -> picked_up ->
    in_transit -> out_for_delivery -> delivered / exception)
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from fleet_simulator import generate_fleet, Truck
from geo_utils import build_route_waypoints, get_position_at_distance
from reference_data import VEHICLE_SPECS

random.seed()  # real randomness for live demo runs (unlike batch_generator, which is seeded for reproducibility)

DELIVERY_STATES = ["created", "picked_up", "in_transit", "out_for_delivery", "delivered"]
ANOMALY_TYPES = ["engine_overheat", "cold_chain_breach", "fuel_drop"]


class TruckRuntimeState:
    """Live in-memory state for one truck during a streaming session."""
    def __init__(self, truck: Truck):
        self.truck = truck
        self.waypoints = build_route_waypoints(truck.home_route)
        self.total_km = self.waypoints[-1]["cumulative_km"]
        self.distance_km = random.uniform(0, self.total_km * 0.3)  # start somewhere early on the route
        self.direction = 1  # 1 = forward, -1 = returning
        self.fuel_level = VEHICLE_SPECS[truck.vehicle_type]["fuel_tank_capacity_litres"] * random.uniform(0.7, 1.0)
        self.delivery_state_idx = random.randint(0, 2)
        self.trip_id = f"TRIP{truck.vehicle_id}{int(time.time())}"
        self.active_anomaly = None
        self.anomaly_ticks_remaining = 0

    def tick(self, seconds_elapsed: int) -> Dict:
        spec = VEHICLE_SPECS[self.truck.vehicle_type]
        pos = get_position_at_distance(self.waypoints, self.distance_km)

        # synthetic speed: real speed limit as ceiling, with realistic variation
        speed_kmph = max(20, min(pos["speed_limit_kmph"], random.gauss(pos["speed_limit_kmph"] * 0.85, 8)))
        distance_increment = speed_kmph * (seconds_elapsed / 3600.0)
        self.distance_km += self.direction * distance_increment

        # bounce back and forth along the route (simulates round trips without needing new trip logic)
        if self.distance_km >= self.total_km:
            self.distance_km = self.total_km
            self.direction = -1
        elif self.distance_km <= 0:
            self.distance_km = 0
            self.direction = 1

        # fuel consumption (real fuel economy from reference_data, synthetic consumption event)
        self.fuel_level -= distance_increment / spec["avg_fuel_economy_kmpl"]
        self.fuel_level = max(0, self.fuel_level)

        # small chance to start/continue/clear an anomaly
        if self.active_anomaly is None and random.random() < 0.01:
            self.active_anomaly = random.choice(
                ["cold_chain_breach"] if self.truck.vehicle_type == "refrigerated_truck" else ["engine_overheat", "fuel_drop"]
            )
            self.anomaly_ticks_remaining = random.randint(3, 8)
        elif self.active_anomaly is not None:
            self.anomaly_ticks_remaining -= 1
            if self.anomaly_ticks_remaining <= 0:
                self.active_anomaly = None

        engine_temp = round(random.uniform(*spec["engine_temp_normal_c"]), 1)
        if self.active_anomaly == "engine_overheat":
            engine_temp = round(spec["engine_temp_overheat_c"] + random.uniform(2, 15), 1)

        cargo_temp = None
        if self.truck.vehicle_type == "refrigerated_truck":
            cargo_temp = round(random.uniform(*spec["cargo_temp_normal_c"]), 1)
            if self.active_anomaly == "cold_chain_breach":
                cargo_temp = round(spec["cargo_temp_breach_c"] + random.uniform(1, 6), 1)

        if self.active_anomaly == "fuel_drop":
            self.fuel_level = max(0, self.fuel_level - random.uniform(5, 15))  # simulated sudden drop (theft/leak)

        now = datetime.now(timezone.utc).isoformat()

        gps_event = {
            "event_type": "gps_ping",
            "trip_id": self.trip_id,
            "vehicle_id": self.truck.vehicle_id,
            "driver_id": self.truck.driver_id,
            "timestamp": now,
            "lat": pos["lat"],
            "lon": pos["lon"],
            "speed_kmph": round(speed_kmph, 1),
            "heading": "forward" if self.direction == 1 else "return",
            "distance_traveled_km": round(self.distance_km, 2),
            "route_name": self.truck.home_route,
        }

        sensor_event = {
            "event_type": "sensor_reading",
            "trip_id": self.trip_id,
            "vehicle_id": self.truck.vehicle_id,
            "timestamp": now,
            "engine_temp_c": engine_temp,
            "fuel_level_litres": round(self.fuel_level, 1),
            "cargo_temp_c": cargo_temp,
            "anomaly_flag": self.active_anomaly,
        }

        delivery_event = None
        if random.random() < 0.05 and self.delivery_state_idx < len(DELIVERY_STATES) - 1:
            self.delivery_state_idx += 1
            delivery_event = {
                "event_type": "delivery_event",
                "trip_id": self.trip_id,
                "vehicle_id": self.truck.vehicle_id,
                "timestamp": now,
                "status": DELIVERY_STATES[self.delivery_state_idx],
                "location_city": pos["nearest_segment_end_city"],
            }

        return {"gps": gps_event, "sensor": sensor_event, "delivery": delivery_event}


def run_file_mode(fleet, output_dir: Path, duration_minutes: int, tick_seconds: int):
    states = [TruckRuntimeState(t) for t in fleet]
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "gps_events").mkdir(exist_ok=True)
    (output_dir / "sensor_events").mkdir(exist_ok=True)
    (output_dir / "delivery_events").mkdir(exist_ok=True)

    end_time = time.time() + duration_minutes * 60
    batch_num = 0

    print(f"Streaming for {duration_minutes} minutes, 1 tick every {tick_seconds}s, {len(fleet)} trucks active...")
    print(f"Writing to: {output_dir.resolve()}")
    print("Point Structured Streaming readStream at these subfolders. Ctrl+C to stop early.\n")

    try:
        while time.time() < end_time:
            gps_batch, sensor_batch, delivery_batch = [], [], []
            for state in states:
                events = state.tick(tick_seconds)
                gps_batch.append(events["gps"])
                sensor_batch.append(events["sensor"])
                if events["delivery"]:
                    delivery_batch.append(events["delivery"])

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            with open(output_dir / "gps_events" / f"batch_{ts}_{batch_num}.json", "w") as f:
                for e in gps_batch:
                    f.write(json.dumps(e) + "\n")
            with open(output_dir / "sensor_events" / f"batch_{ts}_{batch_num}.json", "w") as f:
                for e in sensor_batch:
                    f.write(json.dumps(e) + "\n")
            if delivery_batch:
                with open(output_dir / "delivery_events" / f"batch_{ts}_{batch_num}.json", "w") as f:
                    for e in delivery_batch:
                        f.write(json.dumps(e) + "\n")

            anomalies = [s.truck.vehicle_id for s in states if s.active_anomaly]
            print(f"[{ts}] batch {batch_num}: {len(gps_batch)} gps, {len(sensor_batch)} sensor, "
                  f"{len(delivery_batch)} delivery events"
                  + (f" | ANOMALIES: {anomalies}" if anomalies else ""))

            batch_num += 1
            time.sleep(tick_seconds)
    except KeyboardInterrupt:
        print("\nStopped early by user.")

    print(f"\nDone. {batch_num} batches written to {output_dir.resolve()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file", "kafka"], default="file")
    parser.add_argument("--output", type=str, default="./stream_output")
    parser.add_argument("--duration-minutes", type=int, default=10)
    parser.add_argument("--tick-seconds", type=int, default=10)
    parser.add_argument("--num-trucks", type=int, default=12)
    args = parser.parse_args()

    fleet = generate_fleet(args.num_trucks)

    if args.mode == "file":
        run_file_mode(fleet, Path(args.output), args.duration_minutes, args.tick_seconds)
    else:
        raise NotImplementedError(
            "Kafka mode not required for this project. File-mode streaming via Auto Loader "
            "is a legitimate, commonly used real-time ingestion pattern on its own."
        )


if __name__ == "__main__":
    main()
