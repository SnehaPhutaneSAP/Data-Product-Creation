# Data Engineering Project Bootstrap Script

This repository provides a Python automation script, `setup_project.py`, to create and bootstrap a new Data Engineering project from the `I758889/lob-onestop-shop-bootsrap-template` template.

## Key Features

✨ **Fully Automated Setup**
- Installs Python dependencies automatically (no separate `pip install` needed)
- Validates repository names (lowercase, numbers, hyphens only)
- Sets SAP Artifactory URL automatically (no manual input required)
- Fills `cookiecutter.json` with project values automatically
- Copies base classes and generates transformer templates

**Complete Workflow**
- Repository creation from template via GitHub API
- Transformation setup bootstrap (`npm start` flow)
- Dev container configuration and build
- Bootstrap script execution inside container
- Base class and transformer template setup
- Clear guidance for VS Code and FOS task execution

## Prerequisites

Ensure the following are installed and configured:

- **Python 3.6+**: Install via `brew install python` on macOS.
- **Git**: Install via `brew install git` on macOS.
- **Node.js (16+)**: Install via `brew install node` on macOS.
- **Docker Desktop**: Download and install from [https://www.docker.com/get-started/](https://www.docker.com/get-started/). Ensure it's running.
- **make**: Required for `make init` during template bootstrap.
- **VS Code**: Required for installing the CAPDerivedDataProducts extension. Download from [https://code.visualstudio.com/](https://code.visualstudio.com/). Ensure the CLI is available (`code` command).
- **GitHub Access Token**: Create a personal access token with repo permissions from your GitHub Enterprise instance (e.g., https://github.tools.sap/settings/tokens). This token must have access to the I758889 organization and the template repository.

## Quick Start

1. Clone or navigate to this repository.

2. Create and activate a virtual environment:

**macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**
```cmd
python -m venv venv
venv\Scripts\activate
```

3. Run the setup script (that's it!):

**macOS / Linux:**
```bash
python3 setup_project.py
```

**Windows:**
```cmd
python setup_project.py
```

> **Note**: Python dependencies are installed automatically at startup. No manual `pip install` needed.

## Usage

1. Activate the virtual environment (if not already active):

**macOS / Linux:**
```bash
source venv/bin/activate
```

**Windows:**
```cmd
venv\Scripts\activate
```

2. Run the script:

**macOS / Linux:**
```bash
python3 setup_project.py
```

**Windows:**
```cmd
python setup_project.py
```

3. Answer the prompts:
   - **GitHub organization** (or your username for personal repos)
   - **Repository name** (lowercase letters, numbers, and hyphens only)
   - **Local path** for cloning (recommended: `~/datalake` or `~/projects`)
     - ⚠️ macOS users: Avoid nested paths like `./local/` or `./local2/`. Use simple paths for Docker Desktop compatibility.
   - **GitHub access token** (with repo creation permissions)
   - **SAP Artifactory username**
   - **SAP Artifactory password/token**
   - **Author name** and **email**

**Automatic Setup** (no input required):
- SAP Artifactory Docker registry URL: `common.repositories.cloud.sap`
- Python dependencies installation
- Repository name validation
- Docker path validation

## What the Script Does

**Preparation Phase**
- ✓ Validates all system prerequisites (Python, Git, Node.js, Docker, make)
- ✓ Installs Python dependencies automatically
- ✓ Validates repository name format

**Repository Setup**
- ✓ Creates a new GitHub repository from the template
- ✓ Clones transformation-setup for dev container bootstrap
- ✓ Applies Docker networking hotfixes

**Dev Container Bootstrap**
- ✓ Builds and starts Docker dev container
- ✓ Fills `cookiecutter.json` with your project values
- ✓ Runs `make init` (if available)

**Post-Bootstrap**
- ✓ Copies `ddp_template_base_class/` (base classes for pro-code transformations)
- ✓ Generates a ready-to-use transformer script:
  - Created from `derived_sales_contract_transformation.py` template
  - Renamed to `<repo_name>_transformation.py`
  - Imports automatically updated to match your repository name

## Repository Structure (this repo)

```
Data-Product-Creation/
├── setup_project.py                          # Main bootstrap automation script
├── requirements.txt                          # Python dependencies
├── ddp_template_base_class/                  # Base classes for pro-code DDP transformations
│   ├── base_class.py
│   └── ddp_base_transformation.py
└── transformers/
    └── derived_sales_contract_transformation.py  # Transformer template (used to generate repo-specific transformer)
```

## Generated Repository Structure

After running the script, the generated repository will include (among other things):

```
<repo-name>/
├── ddp_template_base_class/          # Copied from this setup repo
│   ├── base_class.py
│   └── ddp_base_transformation.py
└── transformers/
    └── <repo_name>_transformation.py  # Generated from template with updated imports
```

The transformer file's imports will reference the local `ddp_template_base_class`, for example:

```python
from <repo_name>.ddp_template_base_class.ddp_base_transformation import BaseTransformationJob
```

## Post-Setup Steps

After the script completes:
1. Open the generated repository in VS Code Dev Container (`Cmd+Shift+P` -> `Dev Containers: Open Folder in Container`).
2. Run `Cmd+Shift+P` -> `FOS: Run Task` -> `FOS: Bootstrap a new Data Engineering Project`.
3. Run `Cmd+Shift+P` -> `Dev Containers: Rebuild Container`.
4. Git commit and push your bootstrapped files.
5. Install the CAPDerivedDataProducts extension for DPD file generation:
   - If a file matching `generatedpdfilesfromcds*.vsix` exists in this setup repository, the script auto-installs it using the VS Code CLI (`code --install-extension`).
   - If auto-install is skipped or fails, install manually from VSIX in Visual Studio Code:
     - Open the Extensions panel (Ctrl + Shift + X on Windows).
     - Click the three dots > "Install from VSIX..." and select the `.vsix` file.
     - Restart VS Code if prompted.

## Troubleshooting

- If `python3` is not found, ensure Python is installed: `brew install python`.
- If pip installation fails, ensure you're in the virtual environment.
- The script validates prerequisites automatically and will exit with errors if any are missing.
- If Docker login fails, verify you used the Docker registry host (not the Artifactory web UI URL).
- If template prep lines are shown as skipped, that can be expected when bootstrap cleanup already removed template files in the generated repo.
- **Docker mount errors on macOS** (error: "bind source path does not exist"):
  - **Recommended**: Re-run the script and use `~/datalake` as the clone path.
  - **Alternative**: Share the project folder in Docker Desktop > Preferences > Resources > File Sharing, then retry.
  - Do NOT use nested paths like `./local/` or `./local2/` under the script directory.

## Requirements

See `requirements.txt` for Python packages and system prerequisites.