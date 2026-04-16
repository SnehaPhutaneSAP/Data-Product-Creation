# Data Engineering Project Bootstrap Script

This repository provides a Python automation script, `setup_project.py`, to create and bootstrap a new Data Engineering project from the `bdc-fos/data-engineering-project-bootstrap-template`.

The script covers:
- Repository creation from template
- Transformation setup bootstrap (`npm start` flow)
- Template prep steps (`cookiecutter` + `make init` when available)
- Clear next steps for VS Code Dev Containers and FOS task execution

## Prerequisites

Ensure the following are installed and configured:

- **Python 3.6+**: Install via `brew install python` on macOS.
- **Git**: Install via `brew install git` on macOS.
- **Node.js (16+)**: Install via `brew install node` on macOS.
- **Docker Desktop**: Download and install from [https://www.docker.com/get-started/](https://www.docker.com/get-started/). Ensure it's running.
- **make**: Required for `make init` during template bootstrap.
- **VS Code**: Required for installing the CAPDerivedDataProducts extension. Download from [https://code.visualstudio.com/](https://code.visualstudio.com/). Ensure the CLI is available (`code` command).
- **GitHub Access Token**: Create a personal access token with repo permissions from your GitHub Enterprise instance (e.g., https://github.tools.sap/settings/tokens). This token must have access to the bdc-fos organization and the template repository.

## Quick Start

1. Clone or navigate to this repository.

2. Create and activate a virtual environment (recommended):
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install Python dependencies:
```bash
python3 -m pip install -r requirements.txt
```

## Usage

1. Activate the virtual environment (if not already active):
```bash
source venv/bin/activate
```

2. Run the script:
```bash
python3 setup_project.py
```

3. Provide values for prompts:
   - GitHub organization name (or your username for personal repos)
   - New repository name (alphabets, numbers, hyphens only)
   - Local path for cloning the project
   - GitHub access token
   - SAP Artifactory Docker registry URL (e.g., https://artifactory.company.com)
   - SAP Artifactory username
   - SAP Artifactory password
   - Author name
   - Author email

## What the Script Does

The script will:
- Check all prerequisites.
- Create a new repository from the template.
- Clone and configure the setup tool.
- Run the automated setup.
- Apply template bootstrap prep in the generated repository:
   - Copy `cookiecutter-template.json` to `cookiecutter.json`.
   - Fill `cookiecutter.json` with your project values.
   - Run `make init`.

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

## Requirements

See `requirements.txt` for Python packages and system prerequisites.