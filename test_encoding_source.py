"""Find where the encoding error is happening."""
import traceback

print("Testing file reads...")

# Test 1: Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ .env loaded")
except Exception as e:
    print(f"✗ .env error: {e}")
    traceback.print_exc()

# Test 2: Load environment
try:
    from utils.env import load_environment
    config = load_environment()
    print("✓ Environment loaded")
    print(f"  Network path: {config.get('network_share_path')}")
except Exception as e:
    print(f"✗ Environment error: {e}")
    traceback.print_exc()

# Test 3: Try to load config.yaml if it exists
try:
    import yaml
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        yaml_config = yaml.safe_load(f)
    print("✓ config.yaml loaded")
except FileNotFoundError:
    print("  (config.yaml not used)")
except Exception as e:
    print(f"✗ config.yaml error: {e}")
    traceback.print_exc()

# Test 4: Try bronze ETL
try:
    from etl.bronze.run_bronze_etl import run_bronze_etl
    print("✓ Bronze ETL imported")
except Exception as e:
    print(f"✗ Bronze ETL import error: {e}")
    traceback.print_exc()
