"""
batch_generator.py

SYNTHETIC batch data generator.

Produces ~45-60 days of historical "daily batch drop" files, simulating
what a logistics company's upstream systems (warehouse system, billing
system, maintenance system) would land in cloud storage every day for
Databricks Auto Loader to pick up.

Output: one folder per data domain, one file per day, e.g.:
  output/shipment_manifests/2026-05-01.json
  output/invoices/2026-05-01.csv
  output/maintenance_logs/2026-05-01.csv
  output/driver_vehicle_master/2026-05-01.csv   (SCD2 source -- occasional changes)

Run:
  python batch_generator.py --days 45 --output ./output
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from fleet_simulator import generate_fleet
from reference_data import DIESEL_PRICE_INR_PER_LITRE, VEHICLE_SPECS

fake = Faker("en_IN")
Faker.seed(7)
random.seed(7)


def write_csv(path: Path, rows: list, fieldnames: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")  # newline-delimited JSON, Auto Loader friendly


def generate_shipment_manifests(day: datetime, fleet, num_shipments_range=(15, 30)) -> list:
    """One manifest row per shipment dispatched that day."""
    rows = []
    num_shipments = random.randint(*num_shipments_range)
    for i in range(num_shipments):
        truck = random.choice(fleet)
        rows.append({
            "manifest_id": f"MAN{day.strftime('%Y%m%d')}{i:04d}",
            "shipment_date": day.strftime("%Y-%m-%d"),
            "vehicle_id": truck.vehicle_id,
            "route_name": truck.home_route,
            "customer_name": fake.company(),
            "destination_city": random.choice(["Pune", "Nashik", "Aurangabad", "Mumbai"]),
            "package_count": random.randint(1, 50),
            "total_weight_kg": round(random.uniform(50, 5000), 1),
            "is_cold_chain": truck.vehicle_type == "refrigerated_truck",
            "priority": random.choices(["standard", "express"], weights=[0.8, 0.2])[0],
        })
    return rows


def generate_invoices(day: datetime, manifests: list) -> list:
    """One invoice per shipment manifest (batch billing system)."""
    rows = []
    for m in manifests:
        base_rate_per_kg = round(random.uniform(8, 15), 2)
        amount = round(m["total_weight_kg"] * base_rate_per_kg, 2)
        if m["priority"] == "express":
            amount *= 1.3
        rows.append({
            "invoice_id": f"INV{m['manifest_id']}",
            "manifest_id": m["manifest_id"],
            "invoice_date": day.strftime("%Y-%m-%d"),
            "customer_name": m["customer_name"],
            "amount_inr": round(amount, 2),
            "payment_status": random.choices(["paid", "pending", "overdue"], weights=[0.7, 0.25, 0.05])[0],
        })
    return rows


def generate_maintenance_logs(day: datetime, fleet, daily_probability=0.15) -> list:
    """Most days, no maintenance. Occasionally a truck gets serviced -- batch data from a maintenance system."""
    rows = []
    for truck in fleet:
        if random.random() < daily_probability:
            issue = random.choice([
                "Routine oil change", "Tyre replacement", "Brake inspection",
                "Engine cooling system check", "Refrigeration unit service", "General inspection",
            ])
            rows.append({
                "log_id": f"MNT{day.strftime('%Y%m%d')}{truck.vehicle_id}",
                "service_date": day.strftime("%Y-%m-%d"),
                "vehicle_id": truck.vehicle_id,
                "issue_type": issue,
                "cost_inr": round(random.uniform(1500, 25000), 2),
                "odometer_km": random.randint(20000, 250000),
                "service_center": fake.company() + " Motors",
            })
    return rows


def generate_driver_vehicle_master_snapshot(day: datetime, fleet, change_probability=0.03) -> list:
    """
    Daily snapshot of driver/vehicle master data, the SCD2 source.
    Most days nothing changes. Occasionally a driver gets reassigned or
    a vehicle status changes -- this is exactly the kind of slowly-changing
    dimension data that justifies SCD Type 2 in the Silver layer.
    """
    rows = []
    for truck in fleet:
        # small chance of a "change event" each day (driver reassignment / status change)
        if random.random() < change_probability:
            truck.driver_phone = fake.phone_number()  # mutate in place -- simulates a real update
        rows.append({
            "snapshot_date": day.strftime("%Y-%m-%d"),
            "vehicle_id": truck.vehicle_id,
            "license_plate": truck.license_plate,
            "vehicle_type": truck.vehicle_type,
            "driver_id": truck.driver_id,
            "driver_name": truck.driver_name,
            "driver_phone": truck.driver_phone,
            "driver_license_no": truck.driver_license_no,
            "home_route": truck.home_route,
            "is_active": truck.is_active,
        })
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--start-date", type=str, default="2026-05-01")
    parser.add_argument("--output", type=str, default="./output")
    parser.add_argument("--num-trucks", type=int, default=12)
    args = parser.parse_args()

    output_root = Path(args.output)
    fleet = generate_fleet(args.num_trucks)
    start = datetime.strptime(args.start_date, "%Y-%m-%d")

    for d in range(args.days):
        day = start + timedelta(days=d)
        date_str = day.strftime("%Y-%m-%d")

        manifests = generate_shipment_manifests(day, fleet)
        invoices = generate_invoices(day, manifests)
        maintenance = generate_maintenance_logs(day, fleet)
        master_snapshot = generate_driver_vehicle_master_snapshot(day, fleet)

        write_json(output_root / "shipment_manifests" / f"{date_str}.json", manifests)
        write_csv(output_root / "invoices" / f"{date_str}.csv", invoices,
                  fieldnames=["invoice_id", "manifest_id", "invoice_date", "customer_name", "amount_inr", "payment_status"])
        if maintenance:
            write_csv(output_root / "maintenance_logs" / f"{date_str}.csv", maintenance,
                      fieldnames=["log_id", "service_date", "vehicle_id", "issue_type", "cost_inr", "odometer_km", "service_center"])
        write_csv(output_root / "driver_vehicle_master" / f"{date_str}.csv", master_snapshot,
                  fieldnames=["snapshot_date", "vehicle_id", "license_plate", "vehicle_type", "driver_id",
                              "driver_name", "driver_phone", "driver_license_no", "home_route", "is_active"])

        print(f"Generated {date_str}: {len(manifests)} manifests, {len(invoices)} invoices, "
              f"{len(maintenance)} maintenance logs, {len(master_snapshot)} master records")

    print(f"\nDone. {args.days} days of batch data written to {output_root.resolve()}")


if __name__ == "__main__":
    main()
