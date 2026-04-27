import json
import os
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import urlparse

try:
    import requests
except ModuleNotFoundError:
    requests = None


DEFAULT_TRANSFORMATION_SETUP_REPO = "https://github.tools.sap/bdc-fos/transformations-setup.git"
DEFAULT_TRANSFORMATION_SETUP_BRANCH = "main"
DEFAULT_SAP_ARTIFACTORY_REGISTRY = "common.repositories.cloud.sap"

HOTFIX_DEVCONTAINER_SERVICE_TS = r"""import {execFile} from 'child_process';
import {promisify} from 'util';
import * as path from 'path';
import * as fs from 'fs';

const execFileAsync = promisify(execFile);

type DevcontainerConfig = {
    build?: {
        context?: string;
        args?: Record<string, string>;
    };
    mounts?: string[];
};

export class DevcontainerService {
    async upAndGetContainerId(projectPath: string): Promise<string> {
        const devcontainerConfig = this.readDevcontainerConfig(projectPath);
        const imageName = `devcontainer-${path.basename(projectPath)}-${Date.now()}`;

        await this.buildImage(projectPath, devcontainerConfig, imageName);
        return this.runContainer(projectPath, devcontainerConfig, imageName);
    }

    private readDevcontainerConfig(projectPath: string): DevcontainerConfig {
        const devcontainerPath = path.join(projectPath, '.devcontainer', 'devcontainer.json');
        const configContent = fs.readFileSync(devcontainerPath, 'utf8');
        return JSON.parse(this.stripJsonComments(configContent)) as DevcontainerConfig;
    }

    private stripJsonComments(content: string): string {
        return content.replace(/\/\/.*|\/\*[\s\S]*?\*\//g, '');
    }

    private resolveTemplateValue(value: string, projectPath: string): string {
        return value.replace(/\$\{([^}]+)\}/g, (_match, placeholder: string) => {
            if (placeholder.startsWith('localEnv:')) {
                const envVar = placeholder.slice('localEnv:'.length);
                return process.env[envVar] ?? '';
            }

            if (placeholder === 'localWorkspaceFolderBasename') {
                return path.basename(projectPath);
            }

            return '';
        });
    }

    private async buildImage(projectPath: string, devcontainerConfig: DevcontainerConfig, imageName: string): Promise<void> {
        const dockerfilePath = path.join(projectPath, '.devcontainer', 'Dockerfile');
        const buildContext = path.resolve(projectPath, '.devcontainer', devcontainerConfig.build?.context ?? '.');
        const buildArgs = devcontainerConfig.build?.args ?? {};
        const dockerArgs = ['build', '-f', dockerfilePath, '-t', imageName];

        for (const [argName, rawValue] of Object.entries(buildArgs)) {
            dockerArgs.push('--build-arg', `${argName}=${this.resolveTemplateValue(rawValue, projectPath)}`);
        }

        dockerArgs.push(buildContext);
        console.log(`Building image ${imageName}...`);
        const {stdout, stderr} = await execFileAsync('docker', dockerArgs);
        if (stdout) console.log(stdout);
        if (stderr) console.error(stderr);
    }

    private parseMount(mountSpec: string, projectPath: string): string | null {
        const entries = mountSpec.split(',').map((entry) => entry.trim()).filter(Boolean);
        const mount = new Map<string, string>();

        for (const entry of entries) {
            const [key, ...valueParts] = entry.split('=');
            if (!key || valueParts.length === 0) {
                continue;
            }
            mount.set(key, this.resolveTemplateValue(valueParts.join('='), projectPath));
        }

        const type = mount.get('type') ?? 'volume';
        const source = mount.get('source');
        const target = mount.get('target');

        if (!source || !target) {
            return null;
        }

        if (type == 'bind' && !fs.existsSync(source)) {
            throw new Error(`Configured bind mount source path does not exist: ${source}`);
        }

        const options = [`type=${type}`, `source=${source}`, `target=${target}`];
        const consistency = mount.get('consistency');
        if (consistency) {
            options.push(`consistency=${consistency}`);
        }

        return options.join(',');
    }

    private async sleep(ms: number): Promise<void> {
        await new Promise((resolve) => setTimeout(resolve, ms));
    }

    private async waitForBindMountPath(bindPath: string): Promise<void> {
        const resolvedPath = fs.realpathSync(bindPath);

        for (let attempt = 1; attempt <= 8; attempt++) {
            try {
                // Use a tiny throwaway container to verify Docker Desktop can mount the host path.
                await execFileAsync('docker', [
                    'run',
                    '--rm',
                    '--mount',
                    `type=bind,source=${resolvedPath},target=/mnt/test`,
                    'alpine:3.20',
                    'sh',
                    '-lc',
                    'test -d /mnt/test',
                ]);
                return;
            } catch (error) {
                const message = String(error);
                const isMissingBindPath = message.includes('bind source path does not exist');
                if (!isMissingBindPath || attempt === 8) {
                    throw new Error(
                        `Docker cannot mount workspace path '${resolvedPath}'. ` +
                        `If this path is under Desktop/iCloud, move the project under a local folder like ~/datalake and retry. Original error: ${message}`
                    );
                }

                await this.sleep(1500);
            }
        }
    }

    private async runContainer(projectPath: string, devcontainerConfig: DevcontainerConfig, imageName: string): Promise<string> {
        if (!fs.existsSync(projectPath)) {
            throw new Error(`Workspace path does not exist before docker run: ${projectPath}`);
        }

        await this.waitForBindMountPath(projectPath);

        const projectName = path.basename(projectPath);
        const containerName = `devcontainer-${projectName}-${Date.now()}`;
        const workspaceMount = `type=bind,source=${projectPath},target=/workspaces/${projectName}`;
        const dockerSocketSource = process.platform === 'win32' ? '//var/run/docker.sock' : '/var/run/docker.sock';
        const dockerArgs = [
            'run',
            '-d',
            '--name',
            containerName,
            '--mount',
            workspaceMount,
            '--mount',
            `type=bind,source=${dockerSocketSource},target=/var/run/docker.sock`,
        ];

        for (const mountSpec of devcontainerConfig.mounts ?? []) {
            const parsedMount = this.parseMount(mountSpec, projectPath);
            if (parsedMount) {
                dockerArgs.push('--mount', parsedMount);
            }
        }

        dockerArgs.push(imageName, 'tail', '-f', '/dev/null');
        console.log(`Starting container ${containerName}...`);
        const {stdout, stderr} = await execFileAsync('docker', dockerArgs);
        if (stderr) console.error(stderr);

        const containerId = stdout.trim();
        if (!containerId) {
            throw new Error('Docker run did not return a container ID.');
        }

        console.log(`Container started with ID: ${containerId}`);
        return containerId;
    }

    async execInContainer(containerId: string, command: string): Promise<void> {
        const {stdout, stderr} = await execFileAsync('docker', ['exec', containerId, 'sh', '-c', command]);
        if (stdout) console.log(stdout);
        if (stderr) console.error(stderr);
        console.log(`Executed in devcontainer: ${command}`);
    }
}
"""

HOTFIX_GITHUB_SERVICE_TS = r"""import simpleGit from 'simple-git';

export class GithubService {
    private git = simpleGit();
    private readonly authToken: string;
    private readonly baseUrl: string;

    constructor() {
        const token = process.env.GITHUB_AUTH_TOKEN;

        if (!token) throw new Error('GITHUB_AUTH_TOKEN is not set in environment variables.');

        this.authToken = token;
        const baseUrl = process.env.GITHUB_BASE_URL;

        if (!baseUrl) throw new Error('GITHUB_BASE_URL is not set in environment variables.');

        this.baseUrl = baseUrl.replace(/\/$/, '');
    }

    async cloneRepo(org: string, repo: string, localPath: string): Promise<void> {
        const repoUrl = `${this.baseUrl}/${org}/${repo}.git`;
        const urlWithToken = repoUrl.replace(/^https:\/\//, `https://${this.authToken}@`);
        await this.git.clone(urlWithToken, localPath);
        console.log(`Repository cloned to ${localPath}`);
    }
}
"""

HOTFIX_FILE_EDITOR_SERVICE_TS = r"""import * as path from 'path';
import * as fs from 'fs';

const hyphenToUnderscore = (str: string) => str.replace(/-/g, '_');

export class FileEditorService {
    // Fill cookiecutter.json directly and fallback to cookiecutter-template.json when needed.
    fillCookiecutterTemplate(repoPath: string, repoName: string): void {
        const cookiecutterPath = path.join(repoPath, 'cookiecutter.json');
        const templatePath = path.join(repoPath, 'cookiecutter-template.json');

        let sourcePath = cookiecutterPath;
        if (!fs.existsSync(sourcePath)) {
            sourcePath = templatePath;
        }

        if (!fs.existsSync(sourcePath)) {
            console.warn(`Cookiecutter file not found: ${cookiecutterPath} or ${templatePath}`);
            return;
        }

        const data = JSON.parse(fs.readFileSync(sourcePath, 'utf-8'));
        data.project_name = repoName;
        data.package_name = hyphenToUnderscore(repoName);

        // Fill from env if present
        if (process.env.COOKIECUTTER_AUTHOR_NAME)
            data.author_name = process.env.COOKIECUTTER_AUTHOR_NAME;

        if (process.env.COOKIECUTTER_AUTHOR_EMAIL)
            data.author_email = process.env.COOKIECUTTER_AUTHOR_EMAIL;

        if (process.env.COOKIECUTTER_PROJECT_DESCRIPTION)
            data.project_description = process.env.COOKIECUTTER_PROJECT_DESCRIPTION;

        if (process.env.COOKIECUTTER_PYTHON_VERSION)
            data.python_version = process.env.COOKIECUTTER_PYTHON_VERSION;

        fs.writeFileSync(cookiecutterPath, JSON.stringify(data, null, 2));
        console.log(`cookiecutter.json written to ${cookiecutterPath}`);
    }
}
"""


def command_name(base_name):
    """Return the platform-specific executable name for shell helper commands."""
    if os.name == "nt" and base_name in {"npm", "code"}:
        return f"{base_name}.cmd"
    return base_name


def command_palette_shortcut():
    """Return the appropriate VS Code command palette shortcut for the current OS."""
    return "Ctrl+Shift+P" if os.name == "nt" else "Cmd+Shift+P"


def prompt_repo_name():
    """Prompt until a valid repository name is provided.

    Allowed characters: lowercase letters, numbers, and hyphens.
    """
    pattern = re.compile(r"^[a-z0-9-]+$")
    while True:
        repo_name = input("Enter new repository name (lowercase letters, numbers, hyphens only): ").strip()

        if pattern.fullmatch(repo_name):
            return repo_name

        print("Invalid repository name.")
        print("Allowed: lowercase letters (a-z), numbers (0-9), and hyphens (-).")
        print("Not allowed: capital letters, underscores (_), spaces, or other special characters.")
        print("Please re-enter the repository name.")


def ensure_python_requirements(script_dir):
    """Install Python dependencies from requirements.txt using the current interpreter."""
    requirements_file = os.path.join(script_dir, "requirements.txt")
    if not os.path.isfile(requirements_file):
        print(f"Warning: requirements.txt not found at {requirements_file}. Continuing without auto-install.")
    else:
        print("Installing Python dependencies from requirements.txt...")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", requirements_file],
                check=True,
            )
            print("✓ Python dependencies are installed.")
        except subprocess.CalledProcessError:
            print("Error: Failed to install Python dependencies from requirements.txt.")
            sys.exit(1)

    global requests
    if requests is None:
        try:
            import requests as requests_module

            requests = requests_module
        except ModuleNotFoundError:
            print("Error: 'requests' is not available after dependency installation.")
            sys.exit(1)


def apply_devcontainer_service_hotfix(transformation_setup_dir):
    """Patch transformation-setup to avoid @devcontainers/cli startup failures across OSes."""

    service_path = os.path.join(
        transformation_setup_dir,
        "src",
        "services",
        "devcontainer-service.ts",
    )

    if not os.path.isfile(service_path):
        print(f"Devcontainer service hotfix skipped: file not found: {service_path}")
        return

    try:
        with open(service_path, "w", encoding="utf-8", newline="\n") as service_file:
            service_file.write(HOTFIX_DEVCONTAINER_SERVICE_TS)
        print("Applied devcontainer service hotfix in transformation-setup.")
    except OSError as error:
        print(f"Warning: could not apply devcontainer service hotfix: {error}")


def apply_github_service_hotfix(transformation_setup_dir):
    """Patch transformation-setup so clone errors are not swallowed and hidden."""
    service_path = os.path.join(
        transformation_setup_dir,
        "src",
        "services",
        "github-service.ts",
    )

    if not os.path.isfile(service_path):
        print(f"Github service hotfix skipped: file not found: {service_path}")
        return

    try:
        with open(service_path, "w", encoding="utf-8", newline="\n") as service_file:
            service_file.write(HOTFIX_GITHUB_SERVICE_TS)
        print("Applied github service hotfix in transformation-setup.")
    except OSError as error:
        print(f"Warning: could not apply github service hotfix: {error}")


def apply_file_editor_service_hotfix(transformation_setup_dir):
    """Patch transformation-setup to fill cookiecutter.json directly."""
    service_path = os.path.join(
        transformation_setup_dir,
        "src",
        "services",
        "file-editor-service.ts",
    )

    if not os.path.isfile(service_path):
        print(f"File editor service hotfix skipped: file not found: {service_path}")
        return

    try:
        with open(service_path, "w", encoding="utf-8", newline="\n") as service_file:
            service_file.write(HOTFIX_FILE_EDITOR_SERVICE_TS)
        print("Applied file editor service hotfix in transformation-setup.")
    except OSError as error:
        print(f"Warning: could not apply file editor service hotfix: {error}")


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

    if os.name != "nt":
        try:
            subprocess.run(["make", "--version"], check=True, capture_output=True)
            print("✓ make is installed.")
        except subprocess.CalledProcessError:
            print("Error: make is not installed. Please install make before continuing.")
            sys.exit(1)
    else:
        print("• make check skipped on Windows. The generated repository setup will continue without it.")

    print("All prerequisites are met.\n")


def apply_template_bootstrap_steps(repo_path, repo_name, author_name, author_email):
    """Apply template bootstrap preparation steps in the generated repository."""
    print("Running template bootstrap preparation in generated repository...")
    if not os.path.isdir(repo_path):
        print(f"Skipping template bootstrap prep: generated repository not found at {repo_path}")
        return

    cookiecutter_file = os.path.join(repo_path, "cookiecutter.json")
    makefile_path = os.path.join(repo_path, "Makefile")

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
        print("Template step skipped: cookiecutter.json not found")

    make_executable = shutil.which("make")
    if os.path.isfile(makefile_path) and make_executable:
        try:
            subprocess.run([make_executable, "init"], check=True, cwd=repo_path)
            print("Template step complete: make init")
        except subprocess.CalledProcessError:
            print("Warning: make init failed. You can run it manually in the generated repository.")
    elif os.path.isfile(makefile_path):
        print("Template step skipped: make is not installed on this machine")
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
        subprocess.run([command_name("code"), "-n", repo_path], check=True, capture_output=True, text=True)
        print(f"Opened generated project in a new VS Code window: {repo_path}")
    except FileNotFoundError:
        print("VS Code CLI ('code') was not found in PATH.")
        print(
            "In VS Code, run: "
            f"{command_palette_shortcut()} -> 'Shell Command: Install code command in PATH', then rerun setup."
        )
    except subprocess.CalledProcessError as error:
        print(f"Warning: Could not open VS Code automatically: {error}")


def validate_docker_mount_path(clone_path, repo_name):
    """Warn if the clone path may have Docker mount issues on macOS."""
    import platform
    if platform.system() != "Darwin":
        return
    
    home_dir = os.path.expanduser("~")
    
    # Check if path is nested under testbootstrap_sp or other project setup directories
    if "testbootstrap_sp" in clone_path or "Data-Product-Creation" in clone_path:
        print("\n⚠️  WARNING: Docker Desktop on macOS mount compatibility issue detected.")
        print(f"   Current path: {clone_path}")
        print("   Paths nested under project setup directories may not mount properly in Docker Desktop.")
        print(f"\n   Recommended solutions (in order of preference):")
        print(f"   1. Use: ~/datalake/{repo_name}")
        print(f"   2. Use: ~/projects/{repo_name}")
        print(f"   3. Share the parent folder in Docker Desktop:")
        print(f"      - Open Docker Desktop > Preferences")
        print(f"      - Go to Resources > File Sharing")
        print(f"      - Add: {os.path.dirname(clone_path)}")
        print(f"      - Click Apply & restart\n")


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
        subprocess.run(
            [command_name("code"), "--install-extension", vsix_path],
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"Installed CAPDerivedDataProducts extension from local VSIX: {vsix_path}")
    except FileNotFoundError:
        print("VSIX auto-install skipped: VS Code CLI ('code') is not available in PATH.")
        print(
            "Install it in VS Code via "
            f"{command_palette_shortcut()} -> 'Shell Command: Install code command in PATH'."
        )
    except subprocess.CalledProcessError as error:
        print("VSIX auto-install failed. You can still install manually from VSIX in VS Code.")
        if error.stderr:
            print(f"Reason: {error.stderr.strip()}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ensure_python_requirements(script_dir)
    check_prerequisites()

    github_org = input("Enter GitHub organization name (or your username for personal repos): ").strip()
    repo_name = prompt_repo_name()

    clone_local_path = os.path.expanduser(input("Enter local path for cloning the project: ").strip())

    github_token = input("Enter GitHub access token: ").strip()
    sap_artifactory_url = DEFAULT_SAP_ARTIFACTORY_REGISTRY
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

    url = "https://github.tools.sap/api/v3/repos/I758889/lob-onestop-shop-bootsrap-template/generate"
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
    validate_docker_mount_path(target_base_path, repo_name)
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

    if not os.path.isdir(os.path.join(transformation_setup_dir, "node_modules")):
        subprocess.run([command_name("npm"), "install"], check=True, cwd=transformation_setup_dir)

    apply_devcontainer_service_hotfix(transformation_setup_dir)
    apply_github_service_hotfix(transformation_setup_dir)
    apply_file_editor_service_hotfix(transformation_setup_dir)

    shutil.copy(
        os.path.join(transformation_setup_dir, "template.env"),
        os.path.join(transformation_setup_dir, ".env"),
    )

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
    env_file_path = os.path.join(transformation_setup_dir, ".env")
    with open(env_file_path, "w", encoding="utf-8") as file:
        file.write(env_content)

    try:
        subprocess.run([command_name("npm"), "start"], check=True, cwd=transformation_setup_dir)
    except subprocess.CalledProcessError:
        print("\n❌ npm start failed.\n")
        print("Most common causes and solutions:\n")
        print("1. DOCKER MOUNT ISSUE (macOS)")
        print(f"   If error mentions 'bind source path does not exist':")
        print(f"   - Re-run with: ~/datalake/{repo_name} as the clone path")
        print(f"   - OR share the folder in Docker Desktop > Preferences > Resources > File Sharing")
        print(f"   - Then retry\n")
        print("2. ARTIFACTORY DOCKER REGISTRY")
        print(f"   - Check .env file: {env_file_path}")
        print(f"   - Verify SAP_ARTIFACTORY_URL is correct (not a web UI URL)")
        print(f"   - Should be: common.repositories.cloud.sap\n")
        print("3. RETRY")
        print(f"   - Run 'npm start' manually in {transformation_setup_dir}")
        print(f"   - Wait a minute and retry (delayed repository access is common)\n")
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
    print(f"   ({command_palette_shortcut()} -> Dev Containers: Open Folder in Container -> select the path above)")
    print("   Important: open that specific folder, NOT its parent.")
    print(f"2. Run {command_palette_shortcut()} -> Dev Containers: Rebuild Container.")
    print("3. Git commit and push your bootstrapped files.")
    print("4. CAPDerivedDataProducts extension installation:")
    print("   - The script tries to auto-install from a local generatedpdfilesfromcds*.vsix file in this setup repository.")
    print("   - If auto-install was skipped/failed, install manually from VSIX in VS Code.")

    open_repo_in_new_vscode_window(target_repo_path)


if __name__ == "__main__":
    main()
