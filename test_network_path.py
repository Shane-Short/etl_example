"""Test network path normalization."""
from utils.env import load_environment

config = load_environment()

print("Network paths in config:")
print(f"  network_share_path: {config.get('network_share_path')}")
print(f"  NETWORK_SHARE_PATH: {config.get('NETWORK_SHARE_PATH')}")
print(f"  NETWORK_SHARE_ROOT: {config.get('NETWORK_SHARE_ROOT')}")

# Test if path exists
import os
path = config.get('network_share_path')
print(f"\nTesting path: {path}")
print(f"  Path exists: {os.path.exists(path)}")

# Try to list directory
try:
    files = os.listdir(path)
    print(f"  ✓ Can list directory ({len(files)} items)")
except Exception as e:
    print(f"  ✗ Cannot access directory: {e}")
