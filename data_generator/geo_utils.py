"""
geo_utils.py

SYNTHETIC engine for moving a truck along REAL road segments.
The road geometry (cities, distances, speed limits) is real (see
reference_data.py). The specific second-by-second position of any
given truck on any given day is synthetic / simulated.

Uses simple great-circle interpolation between real waypoints. This is
a simplification (real roads curve, this assumes straight hops between
waypoints) -- intentional and worth mentioning in your README as a
known limitation / "future improvement: snap to real road geometry via
a routing API".
"""

import math
from typing import List, Tuple

from reference_data import CITIES, get_route_segments


EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Real-world great-circle distance between two lat/long points."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def interpolate_point(lat1: float, lon1: float, lat2: float, lon2: float, fraction: float) -> Tuple[float, float]:
    """Linearly interpolate a point `fraction` of the way from point1 to point2.
    fraction=0 -> point1, fraction=1 -> point2."""
    lat = lat1 + (lat2 - lat1) * fraction
    lon = lon1 + (lon2 - lon1) * fraction
    return lat, lon


def build_route_waypoints(route_name: str) -> List[dict]:
    """
    Expand a named route (e.g. ROUTE_MUM_NASHIK_AURANGABAD) into an ordered
    list of real waypoints with cumulative distance and the real speed
    limit that applies to each segment.
    """
    segments = get_route_segments(route_name)
    waypoints = []
    cumulative_km = 0.0

    # first waypoint = starting city
    first_city_name = segments[0].from_city
    first_city = CITIES[first_city_name]
    waypoints.append({
        "city": first_city.name,
        "lat": first_city.lat,
        "lon": first_city.lon,
        "cumulative_km": 0.0,
        "speed_limit_kmph": segments[0].truck_speed_limit_kmph,
    })

    for seg in segments:
        to_city = CITIES[seg.to_city]
        cumulative_km += seg.distance_km
        waypoints.append({
            "city": to_city.name,
            "lat": to_city.lat,
            "lon": to_city.lon,
            "cumulative_km": cumulative_km,
            "speed_limit_kmph": seg.truck_speed_limit_kmph,
        })

    return waypoints


def get_position_at_distance(waypoints: List[dict], distance_km: float) -> dict:
    """
    Given cumulative distance traveled along a route, return the
    interpolated (synthetic) lat/lon and the (real) speed limit that
    applies at that point on the road.
    """
    total_km = waypoints[-1]["cumulative_km"]
    distance_km = max(0.0, min(distance_km, total_km))

    for i in range(len(waypoints) - 1):
        wp_start, wp_end = waypoints[i], waypoints[i + 1]
        if wp_start["cumulative_km"] <= distance_km <= wp_end["cumulative_km"]:
            seg_len = wp_end["cumulative_km"] - wp_start["cumulative_km"]
            fraction = 0.0 if seg_len == 0 else (distance_km - wp_start["cumulative_km"]) / seg_len
            lat, lon = interpolate_point(wp_start["lat"], wp_start["lon"], wp_end["lat"], wp_end["lon"], fraction)
            return {
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_limit_kmph": wp_end["speed_limit_kmph"],
                "distance_km": round(distance_km, 2),
                "total_km": round(total_km, 2),
                "nearest_segment_end_city": wp_end["city"],
            }

    # fallback: at the very end of the route
    last = waypoints[-1]
    return {
        "lat": last["lat"], "lon": last["lon"],
        "speed_limit_kmph": last["speed_limit_kmph"],
        "distance_km": total_km, "total_km": total_km,
        "nearest_segment_end_city": last["city"],
    }
