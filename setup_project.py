import json
import os
import shutil
import subprocess
import sys
import time
from urllib.parse import urlparse

import requests


DEFAULT_TRANSFORMATION_SETUP_REPO = "https://github.tools.sap/bdc-fos/transformations-setup.git"
DEFAULT_TRANSFORMATION_SETUP_BRANCH = "main"


def check_prerequisites():
    """Check system prerequisites before running the setup."""
    print("Checking prerequisites...")

    if sys.version_info < (3, 6):
        print("Error: Python 3.6 or higher is required.")
        sys.exit(1)

    try:
        subprocess.run(["git", "--version"], check=True, capture_output=True)
        print("✓ Git is installed.")
    except subprocess.CalledProcessError:
        print("Error: Git is not installed. Please install Git from https://git-scm.com/")
        sys.exit(1)

    try:
        result = subprocess.run(["node", "-v"], check=True, capture_output=True, text=True)
        version = result.stdout.strip().lstrip("v")
        major = int(version.split(".")[0])
        if major < 16:
            print(f"Error: Node.js version {version} is too old. Version 16 or higher is required.")
            sys.exit(1)
        print(f"✓ Node.js {version} is installed.")
    except subprocess.CalledProcessError:
        print("Error: Node.js is not installed. Please install Node.js 16 or higher from https://nodejs.org/")
        sys.exit(1)

    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
        subprocess.run(["docker", "ps"], check=True, capture_output=True)
        print("✓ Docker is installed and running.")
    except subprocess.CalledProcessError:
        print("Error: Docker is not installed or not running. Please install and start Docker Desktop from https://www.docker.com/get-started/")
        sys.exit(1)

    try:
        subprocess.run(["make", "--version"], check=True, capture_output=True)
        print("✓ make is installed.")
    except subprocess.CalledProcessError:
        print("Error: make is not installed. Please install make before continuing.")
        sys.exit(1)

    print("All prerequisites are met.\n")


def apply_template_bootstrap_steps(repo_path, repo_name, author_name, author_email):
    """Apply template bootstrap preparation steps in the generated repository."""
    print("Running template bootstrap preparation in generated repository...")
    if not os.path.isdir(repo_path):
        print(f"Skipping template bootstrap prep: generated repository not found at {repo_path}")
        return

    cookiecutter_template = os.path.join(repo_path, "cookiecutter-template.json")
    cookiecutter_file = os.path.join(repo_path, "cookiecutter.json")
    makefile_path = os.path.join(repo_path, "Makefile")

    if os.path.isfile(cookiecutter_template):
        shutil.copy(cookiecutter_template, cookiecutter_file)
        print("Template step complete: copied cookiecutter-template.json to cookiecutter.json")
    else:
        print("Template step skipped: cookiecutter-template.json not found (this can be expected after bootstrap cleanup)")

    if os.path.isfile(cookiecutter_file):
        try:
            with open(cookiecutter_file, "r", encoding="utf-8") as file:
                data = json.load(file)

            data["project_name"] = repo_name
            data["package_name"] = repo_name.replace("-", "_")
            data["author_name"] = author_name
            data["author_email"] = author_email

            with open(cookiecutter_file, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=2)
                file.write("\n")

            print("Template step complete: updated cookiecutter.json with project values")
        except (OSError, json.JSONDecodeError) as error:
            print(f"Warning: could not update cookiecutter.json automatically: {error}")
    else:
        print("Template step skipped: cookiecutter.json not found (this can be expected after bootstrap cleanup)")

    if os.path.isfile(makefile_path):
        try:
            subprocess.run(["make", "init"], check=True, cwd=repo_path)
            print("Template step complete: make init")
        except subprocess.CalledProcessError:
            print("Warning: make init failed. You can run it manually in the generated repository.")
    else:
        print("Template step skipped: Makefile not found in generated repository")


def owner_exists(owner_name, github_token):
    """Return True when the target owner exists as an org or user in GitHub Enterprise."""
    headers = {"Authorization": f"token {github_token}"}

    org_response = requests.get(
        f"https://github.tools.sap/api/v3/orgs/{owner_name}",
        headers=headers,
    )
    if org_response.status_code == 200:
        return True

    user_response = requests.get(
        f"https://github.tools.sap/api/v3/users/{owner_name}",
        headers=headers,
    )
    return user_response.status_code == 200


def normalize_artifactory_registry(raw_value):
    """Normalize Artifactory input into the Docker registry host expected by transformation-setup."""
    value = raw_value.strip()
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = parsed.netloc or parsed.path.split("/")[0]
    path = parsed.path.strip("/")

    # Accept the Artifactory UI URL and convert it to the repo-specific Docker host.
    if host == "common.repositories.cloud.sap" and path.startswith("ui/repos/tree/General/"):
        repo_name = path.split("/")[-1]
        return f"{repo_name}.common.repositories.cloud.sap"

    # Accept the generic host and map it to the known Docker registry used by this bootstrap flow.
    if host == "common.repositories.cloud.sap" and path in ("", "v2"):
        return "bdc-content-factory-docker-testing.common.repositories.cloud.sap"

    return host or value


def wait_for_repo_clone_access(owner_name, repo_name, github_token, timeout_seconds=60):
    """Wait until the newly created repository is reachable via git clone operations."""
    repo_url = f"https://{github_token}@github.tools.sap/{owner_name}/{repo_name}.git"
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        result = subprocess.run(
            ["git", "ls-remote", repo_url],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        time.sleep(3)

    return False


def copy_ddp_template_base_class(script_dir, target_repo_path):
    """Copy the ddp_template_base_class folder into the generated repository."""
    source_dir = os.path.join(script_dir, "ddp_template_base_class")
    if not os.path.isdir(source_dir):
        print(f"Skipping ddp_template_base_class copy: source folder not found at {source_dir}")
        return

    dest_dir = os.path.join(target_repo_path, "ddp_template_base_class")
    if os.path.isdir(dest_dir):
        shutil.rmtree(dest_dir)

    shutil.copytree(source_dir, dest_dir)
    print(f"Copied ddp_template_base_class into generated repository: {dest_dir}")


def copy_transformer_template(script_dir, target_repo_path, repo_name):
    """Copy the transformer template into the generated repository, renaming it and updating imports."""
    template_path = os.path.join(script_dir, "transformers", "derived_sales_contract_transformation.py")
    if not os.path.isfile(template_path):
        print(f"Skipping transformer copy: template not found at {template_path}")
        return

    package_name = repo_name.replace("-", "_")

    with open(template_path, "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace("bdc_ia_ddproducts", package_name)

    dest_dir = os.path.join(target_repo_path, "transformers")
    os.makedirs(dest_dir, exist_ok=True)

    dest_file = os.path.join(dest_dir, f"{package_name}_transformation.py")
    with open(dest_file, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Copied transformer template as: {dest_file}")


def open_repo_in_new_vscode_window(repo_path):
    """Open the generated repository in a new VS Code window if the CLI is available."""
    try:
        subprocess.run(["code", "-n", repo_path], check=True, capture_output=True, text=True)
        print(f"Opened generated project in a new VS Code window: {repo_path}")
    except FileNotFoundError:
        print("VS Code CLI ('code') was not found in PATH.")
        print("In VS Code, run: Cmd+Shift+P -> 'Shell Command: Install code command in PATH', then rerun setup.")
    except subprocess.CalledProcessError as error:
        print(f"Warning: Could not open VS Code automatically: {error}")


def install_local_vsix_extension(script_dir):
    """Install CAPDerivedDataProducts VSIX from the setup repository when available."""
    vsix_candidates = sorted(
        [
            file_name
            for file_name in os.listdir(script_dir)
            if file_name.lower().startswith("generatedpdfilesfromcds") and file_name.lower().endswith(".vsix")
        ],
        reverse=True,
    )

    if not vsix_candidates:
        print("VSIX auto-install skipped: no generatedpdfilesfromcds*.vsix found in setup repository.")
        return

    vsix_path = os.path.join(script_dir, vsix_candidates[0])
    try:
        subprocess.run(["code", "--install-extension", vsix_path], check=True, capture_output=True, text=True)
        print(f"Installed CAPDerivedDataProducts extension from local VSIX: {vsix_path}")
    except FileNotFoundError:
        print("VSIX auto-install skipped: VS Code CLI ('code') is not available in PATH.")
        print("Install it in VS Code via Cmd+Shift+P -> 'Shell Command: Install code command in PATH'.")
    except subprocess.CalledProcessError as error:
        print("VSIX auto-install failed. You can still install manually from VSIX in VS Code.")
        if error.stderr:
            print(f"Reason: {error.stderr.strip()}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    check_prerequisites()

    github_org = input("Enter GitHub organization name (or your username for personal repos): ").strip()
    repo_name = input("Enter new repository name (alphabets, numbers, hyphens): ").strip()

    clone_local_path = os.path.expanduser(input("Enter local path for cloning the project: ").strip())

    github_token = input("Enter GitHub access token: ").strip()
    sap_artifactory_url = input(
        "Enter SAP Artifactory Docker registry URL (e.g., bdc-content-factory-docker-testing.common.repositories.cloud.sap): "
    ).strip()
    artifactory_user = input("Enter SAP Artifactory username: ").strip()
    artifactory_password = input("Enter SAP Artifactory password: ").strip()
    author_name = input("Enter author name: ").strip()
    author_email = input("Enter author email: ").strip()

    test_url = "https://github.tools.sap/api/v3/user"
    test_headers = {"Authorization": f"token {github_token}"}
    test_response = requests.get(test_url, headers=test_headers)
    if test_response.status_code != 200:
        print("Error: Invalid GitHub access token.")
        sys.exit(1)
    print("✓ GitHub token is valid.")

    if not owner_exists(github_org, github_token):
        print(f"Error: GitHub owner '{github_org}' was not found on github.tools.sap. Use the org/user slug, not the display name.")
        sys.exit(1)
    print(f"✓ GitHub owner '{github_org}' is valid.")

    sap_artifactory_url = normalize_artifactory_registry(sap_artifactory_url)
    print(f"✓ Using SAP Artifactory Docker registry host: {sap_artifactory_url}")

    url = "https://github.tools.sap/api/v3/repos/bdc-fos/data-engineering-project-bootstrap-template/generate"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    data = {
        "owner": github_org,
        "name": repo_name,
        "private": True,
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        print(f"Failed to create repository: {response.text}")
        return
    print("Repository created successfully.")

    print("Waiting for the new repository to become available for cloning...")
    if not wait_for_repo_clone_access(github_org, repo_name, github_token):
        print("Error: Repository was created but is not yet cloneable on github.tools.sap. Please retry in a minute.")
        return
    print("✓ New repository is cloneable.")

    target_base_path = os.path.abspath(clone_local_path)
    target_repo_path = os.path.join(target_base_path, repo_name)
    os.makedirs(target_base_path, exist_ok=True)

    if os.path.isdir(target_repo_path) and os.listdir(target_repo_path):
        print(f"Error: Target path '{target_repo_path}' already exists and is not empty.")
        print("Choose a different local clone path or repository name to avoid conflicts.")
        sys.exit(1)

    os.chdir(target_base_path)

    transformation_setup_repo = os.environ.get("TRANSFORMATION_SETUP_REPO_URL", DEFAULT_TRANSFORMATION_SETUP_REPO).strip()
    transformation_setup_branch = os.environ.get("TRANSFORMATION_SETUP_BRANCH", DEFAULT_TRANSFORMATION_SETUP_BRANCH).strip()

    transformation_setup_dir = os.path.join(target_base_path, "transformation-setup")
    if not os.path.isdir(transformation_setup_dir):
        print(f"Cloning transformation-setup from {transformation_setup_repo} (branch: {transformation_setup_branch})...")
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                transformation_setup_branch,
                transformation_setup_repo,
                "transformation-setup",
            ],
            check=True,
        )

    if not os.path.isdir(transformation_setup_dir):
        print(f"Error: transformation-setup directory was not created at {transformation_setup_dir}")
        sys.exit(1)

    required_files = ["package.json", "template.env"]
    missing_files = [
        file_name
        for file_name in required_files
        if not os.path.isfile(os.path.join(transformation_setup_dir, file_name))
    ]
    if missing_files:
        print(
            "Error: transformation-setup is incomplete. Missing required files: "
            + ", ".join(missing_files)
        )
        print(f"Checked path: {transformation_setup_dir}")
        sys.exit(1)

    os.chdir("transformation-setup")

    if not os.path.isdir(os.path.join(transformation_setup_dir, "node_modules")):
        subprocess.run(["npm", "install"], check=True)
    shutil.copy("template.env", ".env")

    env_content = f"""GITHUB_ORG={github_org}
GITHUB_REPO={repo_name}
CLONE_LOCAL_PATH={target_repo_path}
GITHUB_AUTH_TOKEN={github_token}
SAP_ARTIFACTORY_URL={sap_artifactory_url}
ARTIFACTORY_USER={artifactory_user}
ARTIFACTORY_PASSWORD={artifactory_password}
COOKIECUTTER_AUTHOR_NAME={author_name}
COOKIECUTTER_AUTHOR_EMAIL={author_email}
DEVCONTAINER_VOLUME_NAME={repo_name}-devcontainer-volume
GITHUB_BASE_URL=https://github.tools.sap
DATALAKE_DIRNAME=datalake
CLONE_REPO_NAME={repo_name}
"""
    with open(".env", "w", encoding="utf-8") as file:
        file.write(env_content)

    try:
        subprocess.run(["npm", "start"], check=True)
    except subprocess.CalledProcessError:
        print("npm start failed. This can happen because of Docker login issues, delayed repository availability, or devcontainer startup problems.")
        print("To fix:")
        print("1. Check the .env file in transformation-setup/ and ensure SAP_ARTIFACTORY_URL is the correct Docker registry URL (not the web UI).")
        print("2. Confirm the generated repository path exists before devcontainer startup.")
        print("3. Run 'npm start' manually in the transformation-setup/ directory after fixing.")
        print("4. If issues persist, retry after a short delay or contact your Artifactory admin if the problem is registry-related.")
        return

    apply_template_bootstrap_steps(target_repo_path, repo_name, author_name, author_email)
    copy_ddp_template_base_class(script_dir, target_repo_path)
    copy_transformer_template(script_dir, target_repo_path, repo_name)

    repo_link = f"https://github.tools.sap/{github_org}/{repo_name}"

    print("Automated setup completed.")
    print(f"Repository link: {repo_link}")
    print(f"\nGenerated project path: {target_repo_path}\n")

    install_local_vsix_extension(script_dir)

    print("Next steps:")
    print(f"1. Open EXACTLY this folder in VS Code: {target_repo_path}")
    print("   (Cmd+Shift+P -> Dev Containers: Open Folder in Container -> select the path above)")
    print("   Important: open that specific folder, NOT its parent.")
    print("2. Run Cmd+Shift+P -> Dev Containers: Rebuild Container.")
    print("3. Git commit and push your bootstrapped files.")
    print("4. CAPDerivedDataProducts extension installation:")
    print("   - The script tries to auto-install from a local generatedpdfilesfromcds*.vsix file in this setup repository.")
    print("   - If auto-install was skipped/failed, install manually from VSIX in VS Code.")

    open_repo_in_new_vscode_window(target_repo_path)


if __name__ == "__main__":
    main()
