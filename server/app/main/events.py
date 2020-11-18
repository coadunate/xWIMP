from flask import url_for, session
from flask_socketio import emit, send
from .. import socketio

from threading import Thread, Event

# for download_database
import os, shutil, subprocess
from time import sleep

# for run_fastq_watcher
from .utils.FASTQFileHandler import FASTQFileHandler
from .utils.tasks import int_download_database

from .utils.parse import krakenParse

# for run_fasq_watcher
from watchdog.observers import Observer


fileListenerThread = Thread()
thread_stop_event = Event()

REDIRECTION_EXECUTED = False


############### HELPER FUNCTIONS ###############

def run_fastq_watcher(app_loc,minion_loc):
    print("RUNNING FASTQ WATCHER")
    event_handler = FASTQFileHandler(app_loc)
    observer = Observer()
    observer.schedule(event_handler, path=minion_loc, recursive=False)
    observer.start()
    sleep(1)

###############------------------###############


@socketio.on('connect',namespace="/analysis")
def analysis_connected():
    print("CONNECTED TO ANALYSIS")

@socketio.on('disconnect',namespace="/analysis")
def analysis_disconnected():
    # delete the analysis_busy file
    subprocess.call(['rm',session.get('app_location') + 'analysis_busy'])
    print("DISCONNECTED FROM ANALYSIS")

@socketio.on('start_fastq_file_listener', namespace='/analysis')
def start_fastq_file_listener(app_location,minion_location):
    # need visibility of the global thread object
    print("Starting FASTQ File Listener")
    global fileListenerThread

    if not fileListenerThread.isAlive():
        print("Starting the FASTQ file listener thread")
        fileListenerThread = Thread(target=run_fastq_watcher(app_location,minion_location))
        fileListenerThread.daemon = True
        fileListenerThread.start()

@socketio.on('update_sankey_filter', namespace="/analysis")
def update_sankey_filter(app_location, value):
    print("update_sankey_filter")

    app_location = app_location if app_location.endswith('/') else app_location + '/'
    analysis_filter_file_path = app_location + 'centrifuge/sankey.filter'
    with open(analysis_filter_file_path,'w') as analysis_filter_file:
        analysis_filter_file.write(str(value))


    with open(app_location + 'centrifuge/sankey.data','w') as sankey_data_file:
        sankey_data_file.write(str(krakenParse(app_location + 'centrifuge/sankey.filter', app_location + 'centrifuge/final.out.kraken')) + "\n")



def on_raw_message(message):

    status = message['status']
    if(status == "PROGRESS"):
        percent_done = message['result']['percent-done']
        status_message = message['result']['message']

        socketReturn = emit('download_database_status',{'percent_done': percent_done, 'status_message': status_message}, namespace="/analysis")
        print(str(percent_done) + "% [" + status_message + "]")


@socketio.on('download_database', namespace="/")
def download_database(dbinfo,queries,alertInfo,current_idx_file):

    print("IN DOWNLOAD_DATABASE")

    # Location for the applicaiton data directory
    app_location = dbinfo['app_location'] if dbinfo['app_location'].endswith('/') else dbinfo['app_location'] + '/'

    # Firstly, we need to remove any existing files that might exist inside
    # our app data folder, as it is supposed to be empty to begin with.
    for file in os.listdir(app_location):
        file_path = os.path.join(app_location,file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                print("DOWNLOAD_DATABASE: Deleting " + file + " file.")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
                print("DOWNLOAD_DATABASE: Deleting " + file + " directory.")
        except Exception as e:
            print(e)


    # Create an file to indicate that the download is in progress
    download_in_progress = open(app_location + '.download_in_progress','a')

    print(alertInfo)

    with open(app_location + 'alertinfo.cfg','w+') as alert_config_file:
        phone_number = '""' if not alertInfo['phone_number'] else alertInfo['phone_number']
        email_address = '""' if not alertInfo['email_address'] else alertInfo['email_address']
        alert_config_file.write('phone_number = ' + phone_number + '\n')
        alert_config_file.write('email_address = ' + email_address + '\n')
        alert_config_file.write('alert_sequence_threshold = ' + str(alertInfo['alert_sequence_threshold']) + '\n')
        alert_config_file.write('alert_sequences = ' + str(alertInfo['alert_sequences']) + '\n')
        alert_config_file.write('already_alerted = []\n')

    # Create database directory.
    print("DOWNLOAD_DATABADE: Creating database directory.")
    os.makedirs(app_location + 'database')

    # Create centrifuge/runs directory
    print("DOWNLOAD_DATABASE: Creating centrifuge/runs directory.")
    os.makedirs(app_location + 'centrifuge/runs')

    print("TAAB-STAT 21/02/2020")
    res = int_download_database.apply_async(args=(dbinfo,queries))
    print(res)
    print("TAAB-DONE 21/02/2020")
    print(res.get(on_message=on_raw_message, propagate=False))
