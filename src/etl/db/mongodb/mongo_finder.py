import platform
import subprocess
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_PORT = os.getenv("MONGO_PORT")

def is_windows():
    return platform.system().lower() == 'windows'


def is_wsl():
    try:
        with open('/proc/version', 'r') as f:
            return 'microsoft' in f.read().lower()
    except:
        return False


def is_running_in_docker():
    """
    Check if we're running inside a Docker container.

    This is critical for determining the correct MongoDB host:
    - Inside Docker: use 'mongo' (service name from docker-compose)
    - Outside Docker (local dev): use 'localhost'
    """
    # Method 1: Check /proc/1/cgroup (most reliable - checks PID 1)
    try:
        with open("/proc/1/cgroup", "rt") as f:
            content = f.read()
            if "docker" in content or "kubepods" in content:
                print("DEBUG: Found docker/kubepods in /proc/1/cgroup - running in Docker")
                return True
    except FileNotFoundError:
        # Not Linux or /proc not available
        pass
    except Exception as e:
        print(f"DEBUG: Could not read /proc/1/cgroup: {e}")

    # Method 2: Check for .dockerenv file
    if os.path.exists('/.dockerenv'):
        print("DEBUG: Found /.dockerenv file - running in Docker")
        return True

    # Method 3: Check current process cgroup
    try:
        with open('/proc/self/cgroup', 'r') as f:
            content = f.read()
            if 'docker' in content or 'containerd' in content:
                print("DEBUG: Found docker/containerd in /proc/self/cgroup - running in Docker")
                return True
    except:
        pass

    # Method 4: Check environment variable (set by docker-compose)
    if os.getenv('IN_DOCKER') or os.getenv('DOCKER_CONTAINER'):
        print("DEBUG: Found IN_DOCKER env var - running in Docker")
        return True

    # Method 5: Check if hostname matches container ID pattern
    try:
        hostname = os.uname().nodename
        if len(hostname) == 12 and all(c in '0123456789abcdef' for c in hostname):
            print(f"DEBUG: Hostname '{hostname}' looks like container ID - running in Docker")
            return True
    except:
        pass

    print("DEBUG: Not detected as running in Docker - assuming local development")
    return False


def get_docker_container_ip(container_name):
    try:
        # Method 1: Using docker inspect
        result = subprocess.run(
            ['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container_name],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            ip = result.stdout.strip()
            print(f"✓ Found container '{container_name}' at IP: {ip}")
            return ip
            
    except subprocess.TimeoutExpired:
        print(f"Docker command timed out")
    except FileNotFoundError:
        print(f"Docker command not found. Is Docker installed?")
    except Exception as e:
        print(f"Error getting container IP: {e}")
    
    return None


def get_mongo_host():
    """
    Determine the correct MongoDB host based on the environment.

    Priority:
    1. MONGO_HOST environment variable (if explicitly set)
    2. Auto-detection based on environment
    """
    print(f"DEBUG: Environment detection starting...")

    # Check if MONGO_HOST is explicitly set in environment
    explicit_host = os.getenv('MONGO_HOST')
    if explicit_host:
        print(f"✓ Using explicit MONGO_HOST from environment: {explicit_host}")
        return explicit_host

    # Auto-detect environment
    if is_running_in_docker():
        host = 'mongo'  # Default service name in docker-compose
        print(f"✓ Detected Docker container environment")
        print(f"  → Using MongoDB host: {host} (docker-compose service name)")
        return host

    # Running locally (VSCode, terminal, etc.)
    if is_windows():
        print(f"✓ Detected Windows host environment")
        print(f"  → Using MongoDB host: localhost (local development)")
        return "localhost"

    if is_wsl():
        print(f"✓ Detected WSL environment")
        print(f"  → Using MongoDB host: localhost (local development)")
        return "localhost"

    # Linux host - try to find MongoDB container
    container_name = os.getenv("MONGO_CONTAINER_NAME")
    if container_name:
        print(f"✓ Detected Linux host - attempting to find MongoDB container: {container_name}")
        container_ip = get_docker_container_ip(container_name)
        if container_ip:
            print(f"  → Using MongoDB container IP: {container_ip}")
            print(f"  Note: If connection fails, set MONGO_HOST=localhost in .env")
            return container_ip

    # Final fallback
    print(f"✓ Using default MongoDB host: localhost (local development)")
    return "localhost"


def list_mongo_containers():
    try:
        # Get all running containers
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{json .}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    container = json.loads(line)
                    # Check if it's a MongoDB container
                    if 'mongo' in container.get('Image', '').lower() or 'mongo' in container.get('Names', '').lower():
                        containers.append({
                            'name': container.get('Names'),
                            'id': container.get('ID'),
                            'image': container.get('Image'),
                            'ports': container.get('Ports')
                        })
            
            if containers:
                print(f"Found {len(containers)} MongoDB container(s):")
                for c in containers:
                    print(f"   - {c['name']} (ID: {c['id'][:12]}, Image: {c['image']})")
            
            return containers
    except Exception as e:
        print(f"Error listing containers: {e}")
    
    return []


def build_mongo_uri(host):
    """Build MongoDB connection URI"""
    if MONGO_USERNAME and MONGO_PASSWORD:
        # With authentication
        return f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{host}:{MONGO_PORT}/?authSource=admin"
    else:
        # Without authentication
        return f"mongodb://{host}:{MONGO_PORT}/"