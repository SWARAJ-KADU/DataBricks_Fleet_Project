"""
reference_data.py

REAL-WORLD reference data used to ground the simulation in reality.
Everything in this file is sourced from real geography / real published
operational parameters (highway distances, speed limits, vehicle specs).

This is intentionally kept separate from the synthetic generators
(fleet_simulator.py, batch_generator.py) so it's obvious in a code review
or interview which parts of the project are "real" vs. "simulated".

Sources (see docs/data_sources.md for full citations):
- Mumbai-Pune Expressway: ~94.5 km, truck speed limit 80 km/h flat / 60 km/h ghat
- Mumbai-Nashik Expressway (NH 60 corridor): ~150 km
- Nashik-Aurangabad: part of Maharashtra state highway network
- City coordinates: public geographic coordinates (WGS84 lat/long)
"""

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class City:
    name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class RouteSegment:
    """A real road segment between two real cities."""
    from_city: str
    to_city: str
    distance_km: float
    truck_speed_limit_kmph: int   # real posted limit for heavy vehicles
    road_name: str


# ---------------------------------------------------------------------------
# REAL CITIES (WGS84 coordinates) — Maharashtra freight corridor
# ---------------------------------------------------------------------------
CITIES = {
    "Mumbai":     City("Mumbai", 19.0760, 72.8777),
    "Pune":       City("Pune", 18.5204, 73.8567),
    "Nashik":     City("Nashik", 19.9975, 73.7898),
    "Aurangabad": City("Aurangabad", 19.8762, 75.3433),
    "Nagpur":     City("Nagpur", 21.1458, 79.0882),
}

# ---------------------------------------------------------------------------
# REAL ROUTE SEGMENTS — actual highway corridors and posted truck speed limits
# Distances and speed limits reflect real published values for these corridors.
# ---------------------------------------------------------------------------
ROUTE_SEGMENTS: List[RouteSegment] = [
    RouteSegment("Mumbai", "Pune", 94.5, 80, "Mumbai-Pune Expressway (Yashwantrao Chavan Expressway)"),
    RouteSegment("Mumbai", "Nashik", 150.0, 80, "Mumbai-Nashik Expressway / NH 60"),
    RouteSegment("Nashik", "Aurangabad", 180.0, 70, "NH 60 / Maharashtra State Highway"),
    RouteSegment("Pune", "Nashik", 210.0, 70, "NH 60 (Pune-Nashik Highway)"),
    RouteSegment("Pune", "Aurangabad", 235.0, 70, "NH 60 / NH 211 corridor"),
]

# Named delivery routes built from the real segments above.
# Each is a real, drivable corridor — not an arbitrary point-to-point line.
NAMED_ROUTES = {
    "ROUTE_MUM_PUNE":      ["Mumbai", "Pune"],
    "ROUTE_MUM_NASHIK":    ["Mumbai", "Nashik"],
    "ROUTE_NASHIK_AURANGABAD": ["Nashik", "Aurangabad"],
    "ROUTE_PUNE_NASHIK_AURANGABAD": ["Pune", "Nashik", "Aurangabad"],
    "ROUTE_MUM_NASHIK_AURANGABAD": ["Mumbai", "Nashik", "Aurangabad"],
}

# ---------------------------------------------------------------------------
# REAL VEHICLE / OPERATIONAL SPECS
# Representative of commercial truck models commonly used for freight in India
# (e.g. Tata LPT / Ashok Leyland medium trucks). Ranges reflect realistic
# real-world operating parameters, used to bound the synthetic sensor data.
# ---------------------------------------------------------------------------
VEHICLE_SPECS = {
    "medium_truck": {
        "make_model": "Tata LPT 1109 (representative medium truck)",
        "fuel_tank_capacity_litres": 120,
        "avg_fuel_economy_kmpl": 6.5,          # km per litre, loaded
        "engine_temp_normal_c": (80, 95),       # normal operating range
        "engine_temp_overheat_c": 110,          # threshold for anomaly injection
        "max_speed_kmph": 90,
    },
    "refrigerated_truck": {
        "make_model": "Ashok Leyland reefer van (representative cold-chain truck)",
        "fuel_tank_capacity_litres": 150,
        "avg_fuel_economy_kmpl": 5.2,
        "engine_temp_normal_c": (80, 95),
        "engine_temp_overheat_c": 110,
        "cargo_temp_normal_c": (2, 8),          # cold-chain target range
        "cargo_temp_breach_c": 10,              # anomaly threshold
        "max_speed_kmph": 85,
    },
}

# Real-world-grounded diesel price band (INR/litre) used to make
# fuel cost calculations in Gold tables realistic rather than arbitrary.
DIESEL_PRICE_INR_PER_LITRE = (92.0, 98.0)


def get_route_total_distance(route_name: str) -> float:
    """Sum real segment distances for a named multi-hop route."""
    cities = NAMED_ROUTES[route_name]
    total = 0.0
    for i in range(len(cities) - 1):
        seg = next(
            s for s in ROUTE_SEGMENTS
            if {s.from_city, s.to_city} == {cities[i], cities[i + 1]}
        )
        total += seg.distance_km
    return total


def get_route_segments(route_name: str) -> List[RouteSegment]:
    """Return the ordered real segments that make up a named route."""
    cities = NAMED_ROUTES[route_name]
    segments = []
    for i in range(len(cities) - 1):
        seg = next(
            s for s in ROUTE_SEGMENTS
            if {s.from_city, s.to_city} == {cities[i], cities[i + 1]}
        )
        segments.append(seg)
    return segments
