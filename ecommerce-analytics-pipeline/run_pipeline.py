# run_pipeline.py - MASTER SCRIPT (NO EMOJIS)
import subprocess
import sys
import time
import os

def run_script(script_name):
    """Run a Python script and check if it succeeds"""
    print(f"\n{'='*50}")
    print(f"RUNNING: {script_name}")
    print(f"{'='*50}")
    
    # Check if script exists
    if not os.path.exists(script_name):
        print(f"ERROR: Script {script_name} not found!")
        return False
    
    try:
        result = subprocess.run([sys.executable, script_name], check=True, capture_output=True, text=True)
        print(result.stdout)  # Print the output of the script
        
        if result.stderr:
            print(f"WARNING: {result.stderr}")
            
        if result.returncode == 0:
            print(f"SUCCESS: {script_name} COMPLETED SUCCESSFULLY!")
            return True
        else:
            print(f"ERROR: {script_name} FAILED!")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Error running {script_name}: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        return False

def main():
    print("STARTING SEMI-AUTOMATED PIPELINE")
    start_time = time.time()
    
    # Run all scripts in sequence
    scripts = [
        "generate_data.py",
        "etl_pipeline.py", 
        "export_for_powerbi.py"
    ]
    
    success_count = 0
    for script in scripts:
        if run_script(script):
            success_count += 1
        else:
            print(f"ERROR: PIPELINE STOPPED at {script}")
            break
    
    end_time = time.time()
    
    if success_count == len(scripts):
        print(f"\nSUCCESS: PIPELINE COMPLETED IN {end_time - start_time:.2f} SECONDS!")
        print("Your Power BI data is ready!")
    else:
        print(f"\nWARNING: Pipeline partially completed ({success_count}/{len(scripts)} scripts)")

if __name__ == "__main__":
    main()