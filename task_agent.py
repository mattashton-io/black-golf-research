import os
import subprocess
from datetime import datetime
import re
import json

# Configuration
TASK_FILE = "plan/task.md"
TEST_REPORT = "comprehensive_test_report.md"
TARGET_SECTION = "## Current Status"

def get_recent_changes():
    """Retrieves recent git commits."""
    try:
        result = subprocess.run(
            ["git", "log", "-n", "5", "--pretty=format:- %s (%h)"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except Exception as e:
        return f"Error retrieving git log: {e}"

def get_test_summary():
    """Reads the latest test report and extracts the summary."""
    if not os.path.exists(TEST_REPORT):
        return "No test report found. Please run test_agent.py."
    
    try:
        with open(TEST_REPORT, "r") as f:
            content = f.read()
        
        # Extract the Summary section
        match = re.search(r"## Summary\n(.*?)(?=\n##|$)", content, re.S)
        if match:
            return match.group(1).strip()
        return "Test report summary section not found."
    except Exception as e:
        return f"Error reading test report: {e}"

def update_task_md(summary_text):
    """Updates the task.md file with the new status."""
    if not os.path.exists(TASK_FILE):
        print(f"Error: {TASK_FILE} not found.")
        return

    try:
        with open(TASK_FILE, "r") as f:
            lines = f.readlines()

        new_lines = []
        in_status_section = False
        status_updated = False

        for line in lines:
            if line.strip() == TARGET_SECTION:
                new_lines.append(line)
                new_lines.append(f"* **Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                new_lines.append(f"  * **Recent Changes:**\n")
                for change in summary_text['changes'].split('\n'):
                    if change.strip():
                        new_lines.append(f"    {change}\n")
                new_lines.append(f"  * **Test Status:**\n")
                for test_line in summary_text['tests'].split('\n'):
                    if test_line.strip():
                        # Clean up test summary formatting if needed
                        clean_test = test_line.lstrip('- ').strip()
                        new_lines.append(f"    - {clean_test}\n")
                in_status_section = True
                status_updated = True
                continue

            # Skip existing content under "Current Status" until next section or empty line
            if in_status_section:
                if line.startswith("##") or (not line.strip() and any(l.startswith("##") for l in lines[lines.index(line):])):
                    in_status_section = False
                    new_lines.append(line)
                continue

            new_lines.append(line)

        if not status_updated:
            print(f"Warning: '{TARGET_SECTION}' section not found. Appending to end.")
            new_lines.append(f"\n{TARGET_SECTION}\n")
            # Repeat update logic if section was missing... (simplified here)

        with open(TASK_FILE, "w") as f:
            f.writelines(new_lines)
        
        print("Successfully updated task.md")

    except Exception as e:
        print(f"Error updating task.md: {e}")

if __name__ == "__main__":
    print("Gathering status information...")
    changes = get_recent_changes()
    tests = get_test_summary()
    
    status_info = {
        "changes": changes,
        "tests": tests
    }
    
    print("Updating task.md...")
    update_task_md(status_info)
