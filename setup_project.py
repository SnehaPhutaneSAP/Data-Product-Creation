import os
import subprocess
import requests
import shutil
import sys

def check_prerequisites():
    """Check system prerequisites before running the setup."""
    print("Checking prerequisites...")

    # Check Python version
    if sys.version_info < (3, 6):
        print("Error: Python 3.6 or higher is required.")
        sys.exit(1)

    # Check Git
    try:
        subprocess.run(["git", "--version"], check=True, capture_output=True)
        print("✓ Git is installed.")
    except subprocess.CalledProcessError:
        print("Error: Git is not installed. Please install Git from https://git-scm.com/")
        sys.exit(1)

    # Check Node.js
    try:
        result = subprocess.run(["node", "-v"], check=True, capture_output=True, text=True)
        version = result.stdout.strip().lstrip('v')
        major = int(version.split('.')[0])
        if major < 16:
            print(f"Error: Node.js version {version} is too old. Version 16 or higher is required.")
            sys.exit(1)
        print(f"✓ Node.js {version} is installed.")
    except subprocess.CalledProcessError:
        print("Error: Node.js is not installed. Please install Node.js 16 or higher from https://nodejs.org/")
        sys.exit(1)

    # Check Docker
    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
        # Check if Docker daemon is running
        subprocess.run(["docker", "ps"], check=True, capture_output=True)
        print("✓ Docker is installed and running.")
    except subprocess.CalledProcessError:
        print("Error: Docker is not installed or not running. Please install and start Docker Desktop from https://www.docker.com/get-started/")
        sys.exit(1)

    print("All prerequisites are met.\n")

def main():
    check_prerequisites()

    # Prompt for inputs
    github_org = input("Enter GitHub organization name: ")
    repo_name = input("Enter new repository name (alphabets, numbers, hyphens): ")
    clone_local_path = input("Enter local path for cloning the project: ")
    github_token = input("Enter GitHub access token: ")
    sap_artifactory_url = input("Enter SAP Artifactory URL: ")
    author_name = input("Enter author name: ")
    author_email = input("Enter author email: ")

    # Validate GitHub token (optional, simple check)
    test_url = "https://api.github.com/user"
    test_headers = {"Authorization": f"token {github_token}"}
    test_response = requests.get(test_url, headers=test_headers)
    if test_response.status_code != 200:
        print("Error: Invalid GitHub access token.")
        sys.exit(1)
    print("✓ GitHub token is valid.")

    # Create repo from template
    url = "https://api.github.com/repos/bdc-fos/data-engineering-project-bootstrap-template/generate"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    data = {
        "owner": github_org,
        "name": repo_name,
        "private": True
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        print(f"Failed to create repository: {response.text}")
        return
    print("Repository created successfully.")

    # Create local directory if it doesn't exist
    os.makedirs(clone_local_path, exist_ok=True)

    # Change to the clone local path
    os.chdir(clone_local_path)

    # Clone the transformation-setup repository
    subprocess.run(["git", "clone", "https://github.tools.sap/bdc-fos/transformation-setup.git"], check=True)

    # Change to transformation-setup directory
    os.chdir("transformation-setup")

    # Install npm dependencies
    subprocess.run(["npm", "install"], check=True)

    # Copy template.env to .env
    shutil.copy("template.env", ".env")

    # Write the .env file with user-provided values
    env_content = f"""GITHUB_ORG={github_org}
GITHUB_REPO={repo_name}
CLONE_LOCAL_PATH={clone_local_path}
GITHUB_AUTH_TOKEN={github_token}
SAP_ARTIFACTORY_URL={sap_artifactory_url}
COOKIECUTTER_AUTHOR_NAME={author_name}
COOKIECUTTER_AUTHOR_EMAIL={author_email}
"""
    with open(".env", "w") as f:
        f.write(env_content)

    # Run the setup process
    subprocess.run(["npm", "start"], check=True)

    print("Automated setup completed.")
    print("Next steps:")
    print("1. Manually build or rebuild your dev container. Follow the instructions at: Dev Container Build and Rebuild FAQ")
    print("2. Git commit and push your bootstrapped files.")

if __name__ == "__main__":
    main()