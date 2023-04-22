import os
import sched
import shlex
import shutil
import hashlib
import time


def get_md5(filename):
    """Calculate the MD5 hash of a file"""
    with open(filename, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
    md5 = file_hash.hexdigest()
    return md5


def sync_folders(source_folder, replica_folder, log_file, backup_interval, scheduler):
    """Synchronize the source and replica folders"""

    file_copied_counter = 0
    file_removed_counter = 0
    file_identical_counter = 0

    # Get a list of all files in the source folder
    source_files = []
    for root, dirs, files in os.walk(source_folder):
        for name in files:
            source_files.append(os.path.join(root, name))

    # Remove any files or directories in the replica folder that do not exist in the source folder
    for root, dirs, files in os.walk(replica_folder, topdown=False):
        for name in files:
            replica_file = os.path.join(root, name)
            source_file = os.path.join(source_folder, os.path.relpath(replica_file, replica_folder))
            if not os.path.exists(source_file):
                os.remove(replica_file)
                print(f"{replica_file} removed from replica folder")
                file_removed_counter += 1
                with open(log_file, "a") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {replica_file} removed from replica folder\n")
        for name in dirs:
            replica_dir = os.path.join(root, name)
            source_dir = os.path.join(source_folder, os.path.relpath(replica_dir, replica_folder))
            if not os.path.exists(source_dir):
                os.rmdir(replica_dir)
                print(f"{replica_dir} removed from replica folder")
                with open(log_file, "a") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {replica_dir} removed from replica folder\n")

    # Copy each file from the source folder to the replica folder if it doesn't already exist or is different
    for source_file in source_files:
        # Get the relative path of the file in the source folder
        rel_path = os.path.relpath(source_file, source_folder)

        # Get the full path of the file in the replica folder
        replica_file = os.path.join(replica_folder, rel_path)

        # Create the directory structure for the file if it doesn't exist
        replica_dir = os.path.dirname(replica_file)
        if not os.path.exists(replica_dir):
            os.makedirs(replica_dir)

        # Check if the file already exists in the replica folder and has the same MD5 hash
        if os.path.exists(replica_file) and get_md5(source_file) == get_md5(replica_file):
            print(f"{source_file} already exists in replica folder and is identical")
            file_identical_counter += 1
            with open(log_file, "a") as f:
                f.write(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {source_file} already exists in replica folder and is identical\n")
        else:
            # Copy the file from the source folder to the replica folder
            shutil.copy2(source_file, replica_file)
            print(f"{source_file} copied to replica folder")
            file_copied_counter += 1
            with open(log_file, "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {source_file} copied to replica folder\n")

    print(f"Files copied - {file_copied_counter}\n")
    print(f"Files removed - {file_removed_counter}\n")
    print(f"Files unaltered - {file_identical_counter}\n")
    with open(log_file, "a") as f:
        f.write(f"Files copied - {file_copied_counter}\n")
        f.write(f"Files removed - {file_removed_counter}\n")
        f.write(f"Files unaltered - {file_identical_counter}\n")

    # Call for scheduling
    sync_scheduler.enter(backup_interval, 1, sync_folders,
                         (source_folder, replica_folder, log_file, backup_interval, scheduler,))

def validate_input(input_string):
    """Validates input for sync_folder function"""

    input_split = shlex.split(input_string)

    source_folder = input_split[0]
    replica_folder = input_split[1]
    log_file = input_split[2]
    backup_interval = input_split[3]

    # Checks if source_folder is empty
    if not os.path.isdir(source_folder) or not os.listdir(source_folder):
        print(f"{source_folder} does not exist or is empty")
        return

    # Checks if replica_folder exists and creates folder structure if not
    if not os.path.isdir(replica_folder):
        answer = input(f"{replica_folder} does not exist, do you wish to create it? y/n")
        if answer == "y" or answer == "Y":
            os.makedirs(replica_folder)
        elif answer == "n" or answer == "N":
            return
        else:
            print("Invalid answer.")
            return

    # Confirmation if replica_folder contains files
    if os.listdir(replica_folder):
        print(
            f"{replica_folder} is not empty continuing will delete all files not present in {source_folder}. Do you wish to continue? y/n")
        answer = input()
        if answer == "y" or answer == "Y":
            pass
        elif answer == "n" or answer == "N":
            return
        else:
            print("Invalid answer.")
            return

    # Creates log file if only path is given
    if not os.path.isfile(log_file) and not log_file.endswith(".txt"):
        log_file += r"\log.txt"
        input_split[2] = log_file

    # Checks if backup_interval is integer and convert seconds to minutes
    try:
        input_split[3] = int(input_split[3])
        input_split[3] *= 60
    except ValueError:
        print(f"{input_split[3]} is not a integer")
        return

    return input_split

def print_help():
    print("Parameters: \"path\\to\\source\\folder\" \"path\\to\\destination\\folder\" \"path\\to\\log\" backup_interval in minutes")

if __name__ == '__main__':

    sync_scheduler = sched.scheduler(time.time, time.sleep)
    while True:
        params = input("Input parameters:\n")
        if params.lower() == "help":
            print_help()
        else:
            validated_params = validate_input(params)
            if validated_params:
                sync_folders(validated_params[0], validated_params[1], validated_params[2], validated_params[3], sync_scheduler)
                try:
                    sync_scheduler.run()
                except KeyboardInterrupt:
                    print("Sync aborted")
                    list(map(sync_scheduler.cancel, sync_scheduler.queue))
