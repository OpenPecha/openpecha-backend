# ruff: noqa: T201
"""
CLI tool for managing API keys.

Usage:
    python scripts/manage_api_keys.py create --name "My Key"
    python scripts/manage_api_keys.py create --name "App Key" --application "app_id"
    python scripts/manage_api_keys.py list
    python scripts/manage_api_keys.py revoke --id "key_id"
    python scripts/manage_api_keys.py rotate --id "key_id"

Requires NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD environment variables.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "functions"))

from database import Database
from identifier import generate_id


def create_api_key(name: str, application_id: str | None = None) -> None:
    """Create a new API key, optionally bound to an application."""
    key_id = generate_id()

    with Database() as db:
        try:
            created_id, raw_key = db.api_key.create(key_id, name, application_id)
        except ValueError as e:
            print(f"\n❌ {e}\n")
            sys.exit(1)

    print("\n✅ API key created successfully!")
    print(f"   ID:          {created_id}")
    print(f"   Name:        {name}")
    if application_id:
        print(f"   Bound to:    {application_id}")
    else:
        print("   Bound to:    (none - master key)")
    print(f"   API Key:     {raw_key}")
    print("\n⚠️  IMPORTANT: Save this API key now. It will not be shown again!\n")


def list_api_keys() -> None:
    """List all API keys."""
    with Database() as db:
        keys = db.api_key.list_all()

    if not keys:
        print("\nNo API keys found.\n")
        return

    print(f"\n{'ID':<25} {'Name':<25} {'Active':<8} {'Bound To':<25} {'Created':<20}")
    print("-" * 105)

    for key in keys:
        created = str(key.get("created_at", "N/A"))[:19] if key.get("created_at") else "N/A"
        active = "Yes" if key.get("is_active") else "No"
        bound_to = key.get("bound_application_id") or "(master)"

        print(f"{key['id']:<25} {key['name']:<25} {active:<8} {bound_to:<25} {created:<20}")

    print()


def revoke_api_key(key_id: str) -> None:
    """Revoke an API key."""
    with Database() as db:
        success = db.api_key.revoke(key_id)

    if success:
        print(f"\n✅ API key '{key_id}' has been revoked.\n")
    else:
        print(f"\n❌ API key '{key_id}' not found.\n")
        sys.exit(1)


def rotate_api_key(key_id: str) -> None:
    """Generate a new value for an API key."""
    with Database() as db:
        new_key = db.api_key.rotate_key(key_id)

    if new_key:
        print("\n✅ API key rotated successfully!")
        print(f"   ID:          {key_id}")
        print(f"   New API Key: {new_key}")
        print("\n⚠️  IMPORTANT: Save this API key now. It will not be shown again!")
        print("   The old key is now invalid.\n")
    else:
        print(f"\n❌ API key '{key_id}' not found.\n")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage API keys",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s create --name "Master Admin Key"
  %(prog)s create --name "WebBuddhist Key" --application webuddhist
  %(prog)s list
  %(prog)s revoke --id abc123
  %(prog)s rotate --id abc123
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create a new API key")
    create_parser.add_argument("--name", required=True, help="Name for the API key")
    create_parser.add_argument(
        "--application",
        required=False,
        help="Application ID to bind the key to (omit for master key)",
    )

    subparsers.add_parser("list", help="List all API keys")

    revoke_parser = subparsers.add_parser("revoke", help="Revoke an API key")
    revoke_parser.add_argument("--id", required=True, help="API key ID to revoke")

    rotate_parser = subparsers.add_parser("rotate", help="Generate a new value for an API key")
    rotate_parser.add_argument("--id", required=True, help="API key ID to rotate")

    args = parser.parse_args()

    if args.command == "create":
        create_api_key(args.name, args.application)
    elif args.command == "list":
        list_api_keys()
    elif args.command == "revoke":
        revoke_api_key(args.id)
    elif args.command == "rotate":
        rotate_api_key(args.id)


if __name__ == "__main__":
    main()
