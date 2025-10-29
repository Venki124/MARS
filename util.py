import subprocess

def get_project_number():
    # Get active project ID cleanly
    project_id = subprocess.run(
        ["gcloud", "config", "get-value", "project"],
        capture_output=True, text=True
    ).stdout.strip()

    if not project_id:
        print("No active project set. Run: gcloud config set project <PROJECT_ID>")
        return None

    # Get project number cleanly
    result = subprocess.run(
        ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
        capture_output=True, text=True
    )

    project_number = result.stdout.strip()

    if not project_number:
        print("Could not fetch project number. Check project permissions.")
        return None

    return project_number


if __name__ == "__main__":
    project_number = get_project_number()
    if project_number:
        print("Project Number:", project_number)