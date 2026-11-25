import json
import os
from dataclasses import dataclass
from pathlib import Path

from resolveflow.policy_diff import Snapshot


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_RULES = {"delivery.evidence", "delivery.follow_up", "delivery.in_transit", "refund.approval"}


def demo_bind(variable: str, default: str) -> str:
    address = os.getenv(variable, default)
    if address.startswith("127.0.0.1:"):
        return address
    if address.startswith("0.0.0.0:") and os.getenv("ALLOW_INSECURE_CONTAINER_BIND") == "1":
        return address
    raise ValueError(f"{variable} must use IPv4 loopback outside the local Compose setup")


@dataclass(frozen=True)
class Settings:
    customer_token: str
    operator_token: str
    api_bind: str
    guardrail_target: str
    guardrail_timeout: float
    max_inflight: int
    data_dir: Path
    active_policy_version: str
    model_provider: str
    aws_region: str
    bedrock_model_id: str

    @property
    def local_api_target(self) -> str:
        port = self.api_bind.rsplit(":", maxsplit=1)[1]
        return f"127.0.0.1:{port}"

    @classmethod
    def load(cls) -> "Settings":
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
        customer_token = os.getenv("CUSTOMER_TOKEN", "")
        operator_token = os.getenv("OPERATOR_TOKEN", "")
        if (
            len(customer_token) < 24 or len(operator_token) < 24 or customer_token == operator_token
            or not customer_token.isascii() or not operator_token.isascii()
        ):
            raise ValueError("Set two distinct ASCII demo tokens of at least 24 characters in .env")
        provider = os.getenv("MODEL_PROVIDER", "mock")
        model_id = os.getenv("BEDROCK_MODEL_ID", "")
        if provider not in {"mock", "bedrock"} or (provider == "bedrock" and not model_id):
            raise ValueError("Use MODEL_PROVIDER=mock or configure BEDROCK_MODEL_ID for bedrock")
        max_inflight = int(os.getenv("MAX_INFLIGHT_REQUESTS", "32"))
        timeout = float(os.getenv("GUARDRAIL_TIMEOUT_SECONDS", "0.5"))
        if not 1 <= max_inflight <= 1024 or not 0 < timeout <= 10:
            raise ValueError("Invalid request concurrency or guardrail deadline")
        api_bind = demo_bind("API_BIND", "127.0.0.1:50052")
        return cls(
            customer_token, operator_token, api_bind,
            os.getenv("GUARDRAIL_TARGET", "127.0.0.1:50051"), timeout, max_inflight,
            ROOT / os.getenv("DATA_DIR", ".data"), os.getenv("ACTIVE_POLICY_VERSION", "v2"),
            provider, os.getenv("AWS_REGION", "us-east-1"), model_id,
        )


def load_policies(directory: Path = ROOT / "policies") -> dict[str, Snapshot]:
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    versions = {}
    for path in sorted(directory.glob("*.json")):
        if path == manifest_path:
            continue
        snapshot = Snapshot.load(path)
        if (
            snapshot.version in versions or snapshot.follow_up_hours <= 0
            or not REQUIRED_RULES <= {rule.rule_id for rule in snapshot.rules}
        ):
            raise ValueError(f"Invalid policy snapshot: {path.name}")
        versions[snapshot.version] = snapshot
    if {version: snapshot.digest for version, snapshot in versions.items()} != manifest:
        raise ValueError("Policy archive does not match its published manifest")
    return versions
