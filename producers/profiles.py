import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from uuid import uuid4


@dataclass
class BehaviorProfile:
    name: str
    domains: List[str]
    categories: List[str]
    user_agents: List[str]
    methods: List[str]
    method_weights: List[float]
    bytes_out_range: tuple[int, int]
    bytes_in_range: tuple[int, int]
    auth_results: List[str]
    auth_weights: List[float]
    status_codes: List[int]
    status_weights: List[float]


OFFICE_WORKER_PROFILE = BehaviorProfile(
    name="office_worker",
    domains=[
        "mail.google.com", "slack.com", "salesforce.com", "notion.so",
        "docs.google.com", "zoom.us", "cnn.com", "nytimes.com", "jira.atlassian.com"
    ],
    categories=["saas", "productivity", "news", "collaboration", "email"],
    user_agents=[
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15"
    ],
    methods=["GET", "POST"],
    method_weights=[0.85, 0.15],
    bytes_out_range=(200, 3500),
    bytes_in_range=(1000, 45000),
    auth_results=["none", "success", "failure"],
    auth_weights=[0.85, 0.14, 0.01],
    status_codes=[200, 304, 404, 500],
    status_weights=[0.90, 0.06, 0.03, 0.01]
)

DEVELOPER_PROFILE = BehaviorProfile(
    name="developer",
    domains=[
        "github.com", "gitlab.com", "api.github.com", "stackoverflow.com",
        "pypi.org", "registry.npmjs.org", "aws.amazon.com", "docker.io"
    ],
    categories=["technology", "development", "cloud", "repositories"],
    user_agents=[
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "curl/7.88.1",
        "git/2.40.1"
    ],
    methods=["GET", "POST", "PUT", "DELETE"],
    method_weights=[0.70, 0.20, 0.05, 0.05],
    bytes_out_range=(500, 25000),
    bytes_in_range=(2000, 250000),
    auth_results=["none", "success", "failure"],
    auth_weights=[0.75, 0.23, 0.02],
    status_codes=[200, 201, 204, 401, 404, 500],
    status_weights=[0.85, 0.05, 0.04, 0.03, 0.02, 0.01]
)

HEAVY_DOWNLOADER_PROFILE = BehaviorProfile(
    name="heavy_downloader",
    domains=[
        "cdn.fastly.net", "storage.googleapis.com", "video.stream.io",
        "archive.org", "pkg.download.service.net"
    ],
    categories=["cdn", "media", "cloud_storage", "downloads"],
    user_agents=[
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Wget/1.21.3 (linux-gnu)"
    ],
    methods=["GET", "POST"],
    method_weights=[0.95, 0.05],
    bytes_out_range=(150, 1500),
    bytes_in_range=(100000, 8000000),
    auth_results=["none", "success"],
    auth_weights=[0.96, 0.04],
    status_codes=[200, 206, 404],
    status_weights=[0.92, 0.06, 0.02]
)


class SimulatedClient:
    def __init__(self, client_id: str, src_ip: str, profile: BehaviorProfile, rng: random.Random):
        self.client_id = client_id
        self.src_ip = src_ip
        self.profile = profile
        self.rng = rng

    def generate_event(self, ts: Optional[datetime] = None) -> Dict[str, Any]:
        if ts is None:
            ts = datetime.now(timezone.utc)
        elif ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        domain = self.rng.choice(self.profile.domains)
        category = self.rng.choice(self.profile.categories)
        user_agent = self.rng.choice(self.profile.user_agents)
        method = self.rng.choices(self.profile.methods, weights=self.profile.method_weights, k=1)[0]
        status = self.rng.choices(self.profile.status_codes, weights=self.profile.status_weights, k=1)[0]
        auth_result = self.rng.choices(self.profile.auth_results, weights=self.profile.auth_weights, k=1)[0]

        bytes_out = self.rng.randint(*self.profile.bytes_out_range)
        bytes_in = self.rng.randint(*self.profile.bytes_in_range)

        action = "blocked" if status in (401, 403) else "allowed"
        url = f"https://{domain}/path/{self.rng.randint(100, 999)}"

        return {
            "event_id": str(uuid4()),
            "ts": ts.isoformat(),
            "client_id": self.client_id,
            "src_ip": self.src_ip,
            "method": method,
            "url": url,
            "domain": domain,
            "status": status,
            "bytes_out": bytes_out,
            "bytes_in": bytes_in,
            "user_agent": user_agent,
            "action": action,
            "category": category,
            "auth_result": auth_result,
        }


def create_population(num_clients: int = 350, seed: int = 42) -> List[SimulatedClient]:
    """Creates a deterministic pool of simulated clients distributed across profiles."""
    rng = random.Random(seed)
    profiles = [OFFICE_WORKER_PROFILE, DEVELOPER_PROFILE, HEAVY_DOWNLOADER_PROFILE]
    profile_weights = [0.60, 0.30, 0.10]

    clients: List[SimulatedClient] = []
    for i in range(1, num_clients + 1):
        client_id = f"client-{i:04d}"
        # Deterministic IP generation within a private range (10.x.y.z)
        second_octet = (i // 256) % 256
        third_octet = i % 256
        src_ip = f"10.10.{second_octet}.{third_octet}"

        selected_profile = rng.choices(profiles, weights=profile_weights, k=1)[0]
        # Each client gets an independent seeded PRNG so client behavior streams are reproducible
        client_rng = random.Random(seed * 10000 + i)
        clients.append(SimulatedClient(client_id, src_ip, selected_profile, client_rng))

    return clients
