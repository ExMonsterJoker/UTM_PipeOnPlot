import os
import shutil

def copy_files_from_list(list_file_path, destination_folder):
    # Ensure destination folder exists
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        print(f"Created destination folder: '{destination_folder}'")

    # Read the list of file paths
    try:
        with open(list_file_path, 'r', encoding='utf-8') as f:
            file_paths = f.readlines()
    except FileNotFoundError:
        print(f"Error: The file '{list_file_path}' was not found.")
        return

    # Process each file path
    success_count = 0
    fail_count = 0

    for path in file_paths:
        path = path.strip()
        if not path:
            continue  # Skip empty lines

        # Remove quotes if they exist around the path (common when copying paths in Windows)
        path = path.strip('\'"')

        if os.path.isfile(path):
            try:
                # Use copy2 to preserve metadata like creation and modification times
                shutil.copy2(path, destination_folder)
                print(f"Copied: {path}")
                success_count += 1
            except Exception as e:
                print(f"Failed to copy '{path}': {e}")
                fail_count += 1
        else:
            print(f"File not found or path is not a file: {path}")
            fail_count += 1

    print("-" * 30)
    print(f"Process complete. Successfully copied: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define paths relative to the script's directory
    list_txt_path = os.path.join(script_dir, 'list File Path.txt')
    dest_folder_path = os.path.join(script_dir, 'Raw Data')
    
    # Run the copy function
    copy_files_from_list(list_txt_path, dest_folder_path)
