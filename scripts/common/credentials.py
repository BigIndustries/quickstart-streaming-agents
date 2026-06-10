"""
Credential loading and management utilities.

Provides functions for:
- Loading credentials from credentials.env files
- Generating Confluent Cloud API keys via CLI
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import dotenv_values


def load_or_create_credentials_file(root: Path) -> Tuple[Path, Dict[str, str]]:
    """
    Load existing credentials.env or create from example.

    Args:
        root: Project root directory

    Returns:
        Tuple of (credentials file path, credentials dictionary)
    """
    creds_file = root / "credentials.env"
    example_file = root / "credentials.env.example"

    if creds_file.exists():
        return creds_file, dotenv_values(creds_file)

    if example_file.exists():
        shutil.copy(example_file, creds_file)
        example_file.unlink()
        print(f"\nCreated {creds_file} from example template.")
    else:
        creds_file.touch()
        print(f"\nCreated new {creds_file}.")

    return creds_file, {}



def generate_confluent_api_keys(
    prefix: str = "streaming-agents",
) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate Confluent API keys using CLI.

    Creates a service account and generates API keys with OrganizationAdmin role.

    Args:
        prefix: Prefix for service account name (default: "streaming-agents")

    Returns:
        Tuple of (api_key, api_secret) or (None, None) if generation fails
    """
    import json as _json
    try:
        timestamp = str(int(time.time()))[-6:]
        sa_name = f"{prefix}-setup-sa-{timestamp}"

        print(f"Creating service account: {sa_name}...")
        sa_result = subprocess.run(
            ["confluent", "iam", "service-account", "create", sa_name,
             "--description", f"Service account for {prefix} streaming agents setup",
             "--output", "json"],
            capture_output=True, text=True, check=True,
        )
        sa_data = _json.loads(sa_result.stdout)
        sa_id = sa_data.get("id") or sa_data.get("Id") or sa_data.get("resource_id") or ""
        if not sa_id:
            print(f"Error: could not extract SA ID from: {sa_result.stdout[:200]}")
            return None, None

        print(f"  SA id: {sa_id}")
        print("Assigning OrganizationAdmin role...")
        role_result = subprocess.run(
            ["confluent", "iam", "rbac", "role-binding", "create",
             "--principal", f"User:{sa_id}", "--role", "OrganizationAdmin"],
            capture_output=True, text=True,
        )
        if role_result.returncode != 0:
            msg = (role_result.stderr or role_result.stdout).strip()
            print(f"Error: OrganizationAdmin role assignment failed: {msg}")
            print("The service account will not have sufficient permissions for participant setup.")
            return None, None
        print("  ✓ OrganizationAdmin assigned")

        print("Creating Cloud API key...")
        key_result = subprocess.run(
            ["confluent", "api-key", "create",
             "--service-account", sa_id, "--resource", "cloud",
             "--description", f"{prefix} setup key",
             "--output", "json"],
            capture_output=True, text=True, check=True,
        )
        key_data = _json.loads(key_result.stdout)
        api_key = key_data.get("key") or key_data.get("api_key") or ""
        api_secret = key_data.get("secret") or key_data.get("api_secret") or ""
        if not api_key or not api_secret:
            print(f"Error: could not extract API key from: {key_result.stdout[:200]}")
            return None, None

        print("✓ API keys generated successfully!")
        return api_key, api_secret

    except subprocess.CalledProcessError as e:
        print(f"Error generating API keys: {e}")

    return None, None
