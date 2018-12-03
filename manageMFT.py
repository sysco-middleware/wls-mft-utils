"""
The script contains a set of utilities for resending files in Oracle MFT.
Available functionality:
    [1] Resubmit files in bulk using filter: resubmitType, state, artifactName, startTime, endTime and ignoreIds. 
        Supports both DRY and REAL runs.
    [2] Resubmit files by Target IDs (supports reading Target IDs from file)
Usage:
1. Execute: wlst manageMFT.py -loadProperties manageMFT_[ENV].properties
2. Select action
"""
import os
import sys
import random
import datetime
from java.io import File
from java.io import FileOutputStream


def resubmit_files_in_bulk(is_dry):
    """
    This function resubmits files using a filter: resubmitType, state, artifactName, startTime, endTime and ignoreIds.
    In case of dry run (preview_mode = "True") the function returns targetIds and the number of files that would be
        resubmitted in case of a real run.
    """
    while True:
        resubmit_type = raw_input("[INPUT] Enter Resubmit Type (SOURCE, TARGET, TRANSFER_INSTANCE, TARGET_INSTANCE): ")
        if not resubmit_type:
            print(cur_dt() + " [ERROR] Resubmit Type cannot be empty. Please, enter a valid type")
            continue
        else:
            break
    resubmit_type = resubmit_type.strip().upper()
    state = raw_input("[INPUT] Enter Status (FAILED, COMPLETED or ACTIVE):      ")
    state = state.strip().upper()
    artifact_name = raw_input("[INPUT] Enter Artifact name (e.g. Evry_EDIFACT_OSB):     ")
    artifact_name = artifact_name.strip()
    start_time = raw_input("[INPUT] Enter Start time (e.g. 01-01-2018 00:00:00:000): ")
    start_time = start_time.strip()
    end_time = raw_input("[INPUT] Enter End time (e.g. 31-12-2018 23:59:59:999):   ")
    end_time = end_time.strip()
    if is_dry:
        chunk_size = -1
        chunk_delay = -1
        ignore_ids = raw_input("[INPUT] Enter comma separated Tracking IDs to ignore:    ")
        comments = ""
        preview_mode = "True"
        log_message = "resubmit_files (DRY RUN) successfully completed"
    else:
        chunk_size = raw_input("[INPUT] Enter Chunk size (e.g. 100):                     ")
        chunk_size = chunk_size.strip()
        if not chunk_size:
            chunk_size = -1
        chunk_delay = raw_input("[INPUT] Enter Chunk delay in sec (e.g. 10):              ")
        chunk_delay = chunk_delay.strip()
        if not chunk_delay:
            chunk_delay = -1
        ignore_ids = raw_input("[INPUT] Enter comma separated Tracking IDs to ignore:    ")
        ignore_ids = ignore_ids.strip()
        comments = raw_input("[INPUT] Enter Comments (e.g. vimosh file resubmission):  ")
        comments = comments.strip()
        preview_mode = "False"
        log_message = "resubmit_files successfully completed"

    print("")

    try:
        filter_log = ("resubmitType=" + resubmit_type + ", " +
                      "status=" + state + ", " +
                      "artifactName=" + artifact_name + ", " +
                      "startTime=" + start_time + ", " +
                      "endTime=" + end_time + ", " +
                      "chunkSize=" + str(chunk_size) + ", " +
                      "chunkDelay=" + str(chunk_delay) + ", " +
                      "ignoreIds=" + ignore_ids + ", " +
                      "previewMode=" + preview_mode
                      )
        log("INFO", "Filter: (" + filter_log + ")")
        print("")
        
        if is_dry:
            resubmitMessages(resubmitType=resubmit_type, state=state, artifactName=artifact_name, startTime=start_time,
                             endTime=end_time, chunkSize=chunk_size, chunkDelay=chunk_delay, ignoreIds=ignore_ids,
                             comments=comments, previewMode="True")
        else:
            log("INFO", "Running in DRY MODE to get the Target IDs first...")
            # Create a temporary file and stream all output of resubmitMessages there
            tf = File(tmp_file_name)
            fos = FileOutputStream(tf, true)
            oldfos = theInterpreter.getOut()
            theInterpreter.setOut(fos)
            resubmitMessages(resubmitType=resubmit_type, state=state, artifactName=artifact_name, startTime=start_time,
                             endTime=end_time, chunkSize=chunk_size, chunkDelay=chunk_delay, ignoreIds=ignore_ids,
                             comments=comments, previewMode="True")
            # Return output to console
            theInterpreter.setOut(oldfos)
            print("")
            cwd = os.getcwd()
            tmp_file = open(tmp_file_name, "r")
            log("INFO", "Temporary file " + os.path.join(cwd, tmp_file.name) + " created.")
            tmp_file_contents = tmp_file.read()
            tmp_file.close()
            log("INFO", "Removing temporary file " + tmp_file_name + ".")
            os.remove(tmp_file_name)
            log("INFO", "Dry run results:")
            print("")
            log_report(tmp_file_contents)
            tmp_file_lines = tmp_file_contents.split("\n")
            tracking_ids = tmp_file_lines[2].replace("Message Ids: [", "").replace("]", "").split(", ")
            tracking_ids_count = len(tracking_ids)
            if tracking_ids_count > 0:
                confirm_resubmit = raw_input("[INPUT] Are you sure you want to resubmit " + str(tracking_ids_count) +
                                             " messages, Y/N [N]? ")
                if confirm_resubmit.upper() == "Y":
                    log("INFO", "Resubmitting the messages...")
                    resubmitMessages(resubmitType=resubmit_type, state=state, artifactName=artifact_name,
                                     startTime=start_time, endTime=end_time, chunkSize=chunk_size,
                                     chunkDelay=chunk_delay, ignoreIds=ignore_ids, comments=comments,
                                     previewMode="False")
        print("")
        log("INFO", log_message)

    except (WLSTException, ValueError, NameError, Exception, AttributeError, TypeError, JavaException), e:
        log("ERROR", str(e))


def resubmit_files_by_ids():
    """
    This function instance id and the number of files that would be resubmitted.
    """
    while True:
        resubmit_type = raw_input("[INPUT] Enter Resubmit Type (SOURCE, TARGET, TRANSFER_INSTANCE, TARGET_INSTANCE): ")
        if not resubmit_type:
            print(cur_dt() + " [ERROR] Resubmit Type cannot be empty. Please, enter a valid type")
            continue
        else:
            break
    resubmit_type = resubmit_type.strip().upper()
    log("INFO", "Provided Resubmit type: " + resubmit_type)
    id_list_str = raw_input("[INPUT] Provide a list of comma separated Tracking IDs or type 'file': ")

    id_list = []  # Full list of IDs read from the file
    id_list_len = 0  # Number of IDs in the file

    if id_list_str.lower().strip() == "file":
        while True:
            id_file_name = raw_input("[INPUT] Enter the name of the file containing IDs. "
                                     "NB! The file must be located in the same directory as this script: ")
            if os.path.exists(id_file_name) and os.path.getsize(id_file_name):
                id_file = open(id_file_name, "r")
                id_file_contents = id_file.read()
                id_file.close()
                break
            else:
                print(cur_dt() + " [WARNING] The file either does not exist or is empty. Provide another filename.")

        id_file_contents = id_file_contents.replace("\r\n", "\n")  # Convert EOL to Unix format
        id_list = id_file_contents.split("\n")
        id_list_len = len(id_list)
        log("INFO", "List of Tracking IDs was read from " + id_file_name)
    else:
        id_list_str = id_list_str.strip()
        log("INFO", "Provided list of Tracking IDs: " + id_list_str)

    comments = raw_input("[INPUT] Enter Comments (e.g. vimosh file resubmission): ")
    comments = comments.strip()
    log("INFO", "Provided comments: " + comments)

    print("")

    try:
        log("INFO", "Starting resubmission...")
        if id_list_str == "file":
            if id_list_len > MAX_CHUNK_SIZE:
                print("")
                log("INFO", "There are too many ID's in the file. Resubmission will be done in batches of " +
                    str(MAX_CHUNK_SIZE) + " files each.")
                print("")

                # Create a list of batches based on id_list and MAX_CHUNK_SIZE, e.g. [[1, 2, 3], [4, 5, 6], [7, 8]]
                chunk_list = [id_list[i:i + MAX_CHUNK_SIZE] for i in range(0, len(id_list), MAX_CHUNK_SIZE)]
                chunk_list_len = len(chunk_list)

                # Resubmit files in batches
                for i, chunk in enumerate(chunk_list):
                    chunk_size = len(chunk)
                    chunk_comments = comments + " (part " + str(i) + " of " + str(chunk_list_len) + ")"
                    log("INFO", "Resubmitting " + str(chunk_size) + " IDs from batch " + str(i + 1) + " of " +
                        str(chunk_list_len) + "...")
                    id_list_str = ", ".join(chunk)
                    resubmit(resubmitType=resubmit_type, idList=id_list_str, comments=chunk_comments)
                    print("")
            else:
                id_list_str = ", ".join(id_list)
                resubmit(resubmitType=resubmit_type, idList=id_list_str, comments=comments)
        else:
            resubmit(resubmitType=resubmit_type, idList=id_list_str, comments=comments)

        print("")
        log("INFO", "resubmit_files_by_ids successfully completed.")

    except (WLSTException, ValueError, NameError, Exception, AttributeError, TypeError, JavaException), e:
        log("ERROR", str(e))


def cur_dt():
    """
    This function returns current date time in %Y-%m-%d_%H%M%S format, i.e. 2018-08-27_132815
    """
    d = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return d


def log(level, text):
    """
    Function log appends a log string "text" to the log file f in the format: "YYYY-MM-DD HH:mm:SS id#### [LEVEL] text"
    E.g. "2018-09-05 12:22:33 id0010 [INFO] Creating session"
    """
    log_file.write(cur_dt() + " " + ID + " [" + level + "] " + str(text) + "\n")
    print(cur_dt() + " [" + level + "] " + str(text))


def log_report(text):
    """
    Function log_report is used for printing a report line to the standard output and the log file log_file.
    E.g. "2018-09-05 12:22:33 id0010 [INFO] Creating session"
    :param text: string
    """
    log_file.write(text + "\n")
    print(text)


def start_connect(function_name, is_connected):
    """
    This function connection to the given server if not yet connected.
    :type function_name: string. Name of the function that will be started. Used for logging.
    :type is_connected: bool. Connection status: True - connected, False otherwise
    :rtype: bool
    """
    try:
        log("INFO", "======================================================================")
        log("INFO", "Starting " + function_name + "...")
        if not is_connected:
            log("INFO", "Connecting to " + url + " as " + usrname + "...")
            connect(usrname, password, url)
            print("")
            log("INFO", "Connected to " + url + " as " + usrname + ".")
        is_connected = True
        return is_connected

    except (WLSTException, ValueError, NameError, Exception, AttributeError, TypeError), e:
        log("ERROR", str(e))


def main():
    """
    The main function. Prompts to select an action and calls the corresponding function
    """
    try:
        is_connected = False
        while True:
            print("")
            print("[1] Resubmit files in bulk using filter (DRY RUN)")
            print("[2] Resubmit files in bulk using filter")
            print("[3] Resubmit files by Target IDs")
            print("[4] Exit")
            print("")
            procedure = raw_input("[INPUT] Choose what you want to do from the list above: ")
            print("")
            if not procedure:
                print(cur_dt() + " [ERROR] Input cannot be empty. Please, enter a number from 1 to 4.")
            elif procedure == "1":
                is_connected = start_connect("resubmit_files_in_bulk", is_connected)
                resubmit_files_in_bulk(is_dry=True)
            elif procedure == "2":
                is_connected = start_connect("resubmit_files_in_bulk", is_connected)
                resubmit_files_in_bulk(is_dry=False)
            elif procedure == "3":
                is_connected = start_connect("resubmit_files_by_ids", is_connected)
                resubmit_files_by_ids()
            else:
                break
        log_file.close()
        disconnect()
        exit()

    except (WLSTException, ValueError, NameError, Exception, AttributeError, EOFError), e:
        log("ERROR", str(e))
        log_file.close()
        disconnect()
        raise


# Create a four digit random id left padded with zeros for logging
n = random.randint(1, 1000)
ID = "id" + str("%04d" % n)
MAX_CHUNK_SIZE = 100  # Cannot be higher than 850 due to MFT limitations

# Name of the log file is derived from the name of the script
log_file_name = sys.argv[0].replace("py", "log")
tmp_file_name = cur_dt() + "_" + sys.argv[0].replace("py", "tmp")

log_file = open(log_file_name, "a")
print(cur_dt() + " [INFO] Output is sent to " + log_file_name + ". Log ID = " + ID)


main()
