import json
import os
import shutil
import subprocess
import sys

import requests


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


def main():
    check_prerequisites()

    github_org = input("Enter GitHub organization name (or your username for personal repos): ")
    repo_name = input("Enter new repository name (alphabets, numbers, hyphens): ")
    clone_local_path = input("Enter local path for cloning the project: ")
    github_token = input("Enter GitHub access token: ")
    sap_artifactory_url = input(
        "Enter SAP Artifactory Docker registry URL (e.g., bdc-content-factory-docker-testing.common.repositories.cloud.sap): "
    )
    artifactory_user = input("Enter SAP Artifactory username: ")
    artifactory_password = input("Enter SAP Artifactory password: ")
    author_name = input("Enter author name: ")
    author_email = input("Enter author email: ")

    test_url = "https://github.tools.sap/api/v3/user"
    test_headers = {"Authorization": f"token {github_token}"}
    test_response = requests.get(test_url, headers=test_headers)
    if test_response.status_code != 200:
        print("Error: Invalid GitHub access token.")
        sys.exit(1)
    print("✓ GitHub token is valid.")

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

    target_base_path = os.path.abspath(clone_local_path)
    os.makedirs(target_base_path, exist_ok=True)
    os.chdir(target_base_path)

    subprocess.run(["git", "clone", "https://github.tools.sap/bdc-fos/transformation-setup.git"], check=True)
    os.chdir("transformation-setup")

    subprocess.run(["npm", "install"], check=True)
    shutil.copy("template.env", ".env")

    env_content = f"""GITHUB_ORG={github_org}
GITHUB_REPO={repo_name}
CLONE_LOCAL_PATH={target_base_path}
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
        print("npm start failed. This is likely due to Docker login issues with the Artifactory URL.")
        print("To fix:")
        print("1. Check the .env file in transformation-setup/ and ensure SAP_ARTIFACTORY_URL is the correct Docker registry URL (not the web UI).")
        print("2. Run 'npm start' manually in the transformation-setup/ directory after fixing.")
        print("3. If issues persist, contact your Artifactory admin for the correct Docker registry URL.")
        return

    generated_repo_path = os.path.join(target_base_path, repo_name)
    apply_template_bootstrap_steps(generated_repo_path, repo_name, author_name, author_email)

    print("Automated setup completed.")
    print("Next steps:")
    print("1. Open the generated project in VS Code Dev Container (Cmd+Shift+P -> Dev Containers: Open Folder in Container).")
    print("2. Run Cmd+Shift+P -> FOS: Run Task -> FOS: Bootstrap a new Data Engineering Project.")
    print("3. Run Cmd+Shift+P -> Dev Containers: Rebuild Container.")
    print("4. Git commit and push your bootstrapped files.")
    print("5. Install the CAPDerivedDataProducts extension for DPD file generation:")
    print("   - Download the .vsix file: generatedpdfilesfromcds.vsix from https://github.tools.sap/bdc/CAPDerivedDataProducts")
    print("   - Open Visual Studio Code.")
    print("   - Open the Extensions panel (Ctrl + Shift + X on Windows).")
    print("   - Click the three dots > 'Install from VSIX...' and select the downloaded file.")
    print("   - Restart VS Code if prompted.")
    print("   - The extension will be available in the new repo for generating DPD files.")


if __name__ == "__main__":
    main()
