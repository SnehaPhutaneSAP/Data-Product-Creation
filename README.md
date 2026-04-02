# Data Engineering Project Bootstrap Script

This repository contains a Python script (`setup_project.py`) that automates the setup of a new data engineering project using the bdc-fos/data-engineering-project-bootstrap-template and the transformation-setup utility.

## Prerequisites

Ensure the following are installed and configured:

- **Python 3.6+**: Install via `brew install python` on macOS.
- **Git**: Install via `brew install git` on macOS.
- **Node.js (16+)**: Install via `brew install node` on macOS.
- **Docker Desktop**: Download and install from [https://www.docker.com/get-started/](https://www.docker.com/get-started/). Ensure it's running.
- **GitHub Access Token**: Create a personal access token with repo permissions from [https://github.com/settings/tokens](https://github.com/settings/tokens).

## Installation

1. **Clone or navigate to this repository** (if not already done).

2. **Create and activate a virtual environment** (required due to macOS/Homebrew Python restrictions to avoid pip installation errors):
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Python dependencies**:
   ```
   python3 -m pip install -r requirements.txt
   ```

## Usage

1. **Activate the virtual environment** (if not already activated):
   ```
   source venv/bin/activate
   ```

2. **Run the script**:
   ```
   python3 setup_project.py
   ```

3. **Follow the prompts** to enter:
   - GitHub organization name
   - New repository name (alphabets, numbers, hyphens only)
   - Local path for cloning the project
   - GitHub access token
   - SAP Artifactory URL
   - Author name
   - Author email

The script will:
- Check all prerequisites.
- Create a new repository from the template.
- Clone and configure the setup tool.
- Run the automated setup.

## Post-Setup Steps

After the script completes:
1. Manually build or rebuild your dev container. Follow the instructions at: [Dev Container Build and Rebuild FAQ](https://example.com/faq) (replace with actual link).
2. Git commit and push your bootstrapped files.

## Troubleshooting

- If `python3` is not found, ensure Python is installed: `brew install python`.
- If pip installation fails, ensure you're in the virtual environment.
- The script validates prerequisites automatically and will exit with errors if any are missing.

## Requirements

See `requirements.txt` for Python packages and system prerequisites.