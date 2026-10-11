import ipaddress

from common.logging import get_logger
from common.models import Event

logger = get_logger("processor.enrich")

# Static lookup mapping common domains to curated threat/business categories
DOMAIN_CATEGORY_MAP: dict[str, str] = {
    # SaaS and Productivity
    "mail.google.com": "email",
    "docs.google.com": "productivity",
    "slack.com": "collaboration",
    "salesforce.com": "crm",
    "notion.so": "productivity",
    "zoom.us": "video_conferencing",
    "jira.atlassian.com": "issue_tracking",
    
    # Development and Cloud
    "github.com": "development",
    "gitlab.com": "development",
    "api.github.com": "development",
    "stackoverflow.com": "development",
    "pypi.org": "package_repository",
    "registry.npmjs.org": "package_repository",
    "aws.amazon.com": "cloud_infrastructure",
    "docker.io": "container_registry",
    
    # News and Media
    "cnn.com": "news",
    "nytimes.com": "news",
    "video.stream.io": "media_streaming",
    "cdn.fastly.net": "content_delivery",
    "storage.googleapis.com": "cloud_storage",
    
    # Suspicious / Malicious indicators
    "auth.internal-corp.net": "authentication",
    "mega-cloud-drop.io": "unapproved_storage",
    "super-sketchy-pastebin.ru": "suspicious",
    "c2-command-callback.net": "c2_callback",
    "c2-updater-ping.net": "c2_callback"
}


def enrich_event(event: Event) -> Event:
    """Enriches an event with standardized domain categorization and IP metadata."""
    # 1. Normalize domain (lowercase, strip trailing dots)
    clean_domain = event.domain.lower().rstrip(".")
    event.domain = clean_domain

    # 2. Enrich category from static map if recognized, or preserve existing
    if clean_domain in DOMAIN_CATEGORY_MAP:
        event.category = DOMAIN_CATEGORY_MAP[clean_domain]

    # 3. Canonicalize IP address string
    try:
        parsed_ip = ipaddress.ip_address(event.src_ip)
        event.src_ip = parsed_ip.compressed
    except ValueError as val_err:
        logger.debug(f"IP canonicalization skipped for '{event.src_ip}': {val_err}")

    return event
