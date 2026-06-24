"""
fleet_simulator.py

SYNTHETIC fleet definition + journey simulation engine.

Defines a small fleet of trucks/drivers (synthetic identities, via Faker)
that travel along REAL routes (reference_data.py / geo_utils.py).

This module is the shared "world state" used by both:
  - streaming_producer.py  (emits GPS/sensor/delivery events continuously)
  - batch_generator.py     (generates historical batch files for 1-2 months)

Keeping fleet/driver/route definitions here (rather than duplicated in
both producers) ensures batch and streaming data are consistent with
each other -- e.g. the same truck IDs, same driver assignments -- which
matters when you later join Bronze tables across sources in Silver.
"""

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from faker import Faker

from reference_data import VEHICLE_SPECS, NAMED_ROUTES, DIESEL_PRICE_INR_PER_LITRE
from geo_utils import build_route_waypoints, get_position_at_distance

fake = Faker("en_IN")  # Indian locale for realistic names/addresses
Faker.seed(42)
random.seed(42)


@dataclass
class Truck:
    vehicle_id: str
    vehicle_type: str          # "medium_truck" or "refrigerated_truck"
    license_plate: str
    driver_id: str
    driver_name: str
    driver_phone: str
    driver_license_no: str
    home_route: str            # one of NAMED_ROUTES keys
    fuel_tank_capacity: float = field(init=False)
    onboarded_date: datetime = field(default_factory=lambda: datetime(2026, 1, 1))
    is_active: bool = True

    def __post_init__(self):
        self.fuel_tank_capacity = VEHICLE_SPECS[self.vehicle_type]["fuel_tank_capacity_litres"]


def generate_fleet(num_trucks: int = 12) -> List[Truck]:
    """
    Generate a synthetic fleet of trucks with synthetic driver identities,
    assigned to real named routes. ~25% are refrigerated (cold-chain) trucks
    to give the project a believable reason for cold-chain anomaly detection.
    """
    route_names = list(NAMED_ROUTES.keys())
    fleet = []

    for i in range(1, num_trucks + 1):
        vehicle_type = "refrigerated_truck" if i % 4 == 0 else "medium_truck"
        state_code = random.choice(["MH12", "MH14", "MH20", "MH04"])  # real Maharashtra RTO codes
        plate = f"{state_code}-{random.randint(1000,9999)}"

        truck = Truck(
            vehicle_id=f"TRK{i:03d}",
            vehicle_type=vehicle_type,
            license_plate=plate,
            driver_id=f"DRV{i:03d}",
            driver_name=fake.name_male() if random.random() > 0.1 else fake.name_female(),
            driver_phone=fake.phone_number(),
            driver_license_no=fake.bothify(text="MH##??#######").upper(),
            home_route=random.choice(route_names),
        )
        fleet.append(truck)

    return fleet


@dataclass
class JourneyState:
    """Tracks a truck's progress along its route for one journey/trip."""
    trip_id: str
    truck: Truck
    route_name: str
    start_time: datetime
    distance_traveled_km: float = 0.0
    fuel_level_litres: float = 0.0
    avg_speed_kmph: float = 0.0
    status: str = "in_transit"          # in_transit, completed, delayed, anomaly
    anomaly_active: Optional[str] = None  # None, "engine_overheat", "cold_chain_breach", "fuel_drop"

    def __post_init__(self):
        self.fuel_level_litres = self.truck.fuel_tank_capacity * random.uniform(0.85, 1.0)


def estimate_trip_duration_hours(route_name: str) -> float:
    """Realistic trip duration using real distance and real speed limits,
    with synthetic variability for traffic/rest stops."""
    waypoints = build_route_waypoints(route_name)
    total_km = waypoints[-1]["cumulative_km"]
    avg_speed = sum(w["speed_limit_kmph"] for w in waypoints[1:]) / max(1, len(waypoints) - 1)
    avg_speed *= 0.75  # real trucks run below speed limit on average (traffic, loading, rest stops)
    return round(total_km / avg_speed, 2)
