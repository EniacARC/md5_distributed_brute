import hashlib


def check_md5(num, target_hash, running):
    """Calculates MD5 and checks if it matches the target hash."""
    if running.is_set():
        return None  # Stop early if hash is already found

    # Calculate the MD5 hash for the number
    md5_hash = hashlib.md5(str(num).encode()).hexdigest()

    # DEBUG: Print the number and its MD5 hash
    print(f"Checking number: {num}, MD5: {md5_hash}")

    if md5_hash == target_hash:
        print(f"Found match: {num}")  # DEBUG: Output when a match is found
        running.set()  # Set the flag to stop other processes
        return num  # Return the found number

    return None
