"""
Login verification utilities for Confluent and cloud providers.

Provides functions for:
- Checking Confluent CLI login status
- Checking AWS CLI login status
- Checking Azure CLI login status
"""

import subprocess
import sys
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values


def check_confluent_login() -> bool:
    """
    Check if user is logged into Confluent CLI.

    Returns:
        True if logged in, False otherwise
    """
    try:
        result = subprocess.run(
            ["confluent", "environment", "list"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.returncode == 0
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def attempt_confluent_auto_login(creds: dict) -> bool:
    """Try to log into Confluent Cloud using a credentials dict.

    Reads CONFLUENT_EMAIL and CONFLUENT_PASSWORD from the provided dict.
    Returns True on success, False if credentials are missing or login fails.
    """
    email = creds.get("CONFLUENT_EMAIL")
    password = creds.get("CONFLUENT_PASSWORD")
    if not email or not password:
        return False
    result = subprocess.run(
        ["confluent", "login", "--save"],
        input=f"{email}\n{password}\n",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  Auto-login failed (exit {result.returncode}):")
        if result.stderr.strip():
            print(f"  {result.stderr.strip()}")
        if result.stdout.strip():
            print(f"  {result.stdout.strip()}")
        return False
    return check_confluent_login()


def _attempt_login_quiet(email: str, password: str) -> bool:
    """Attempt Confluent login without printing error output. Returns True on success."""
    result = subprocess.run(
        ["confluent", "login", "--save"],
        input=f"{email}\n{password}\n",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False
    return check_confluent_login()


def confluent_login_interactive() -> None:
    """Ensure Confluent CLI login, launching a browser if the session has expired.

    Skips the browser prompt if already authenticated. Exits with an error if
    login fails (e.g. CLI not installed, network issue).
    """
    if check_confluent_login():
        print("✓ Already logged into Confluent Cloud")
        return

    print("Opening Confluent Cloud login in your browser...")
    result = subprocess.run(["confluent", "login", "--save"])

    if result.returncode != 0 or not check_confluent_login():
        print("\nError: Confluent login failed.")
        print("  If your organisation uses SSO, run:  confluent login --sso")
        sys.exit(1)

    print("✓ Logged into Confluent Cloud")


def ensure_confluent_login(creds: Optional[dict] = None) -> None:
    """Check CLI login → attempt auto-login from creds → exit(1) with clear instructions."""
    if check_confluent_login():
        return
    if creds is None:
        creds = dotenv_values(
            str(Path(__file__).parent.parent.parent / "credentials.env")
        )
    if attempt_confluent_auto_login(creds):
        return
    print("\nError: Not logged into Confluent Cloud.")
    print("Please run: confluent login")
    print("  (or rerun `uv run setup` to save credentials for auto-login)")
    print("  (SSO accounts: run `confluent login --sso`)")
    print(
        "  (or delete CONFLUENT_EMAIL/CONFLUENT_PASSWORD from credentials.env to re-prompt)"
    )
    sys.exit(1)
