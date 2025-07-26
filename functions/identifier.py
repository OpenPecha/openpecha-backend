import secrets


def generate_id(size: int = 16) -> str:
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz-"
    return "".join(secrets.choice(chars) for _ in range(size))
