from celery import Celery
import subprocess, os, shutil, random
from celery.exceptions import SoftTimeLimitExceeded
from flask_socketio import emit

from Bio import Entrez

celery = Celery('tasks', broker='redis://localhost:6379',backend='redis')

Entrez.email = 'tayab.soomro@usask.ca'

@celery.task(bind=True,name='app.main.tasks.int_download_database')
def int_download_database(self,db_data,queries):
    print("YOYOYO-21/02/2020")
    os.makedirs("/Users/soomrot/insideFolderToBeDeleted")
    app_location = db_data['app_location']
    bacteria = db_data['bacteria']
    archaea = db_data['archaea']
    virus = db_data['virus']

    # add trailing slashes if they don't exist
    app_location = app_location if app_location.endswith('/') else app_location + '/'

    # Create variables for locations.
    app_location_database = app_location + 'database/'

    # Download taxonomy
    print("DOWNLOAD DATABASE: Downloading taxonomy files")
    self.update_state(state="PROGRESS", meta={'percent-done': 10, 'message': 'Downloading taxonomy files'})

    tax_cmd = ['centrifuge-download -o ' + app_location_database + 'taxonomy taxonomy']
    try:
        tax_cmd_output = subprocess.Popen(tax_cmd,shell=True,stderr=subprocess.STDOUT)
        tax_cmd_output.communicate()
        tax_cmd_output.wait()
    except (OSError, subprocess.CalledProcessError) as exception:
            print(str(exception))
            self.update_state(state="FAILURE", meta={'percent-done': 10, 'message': 'ERROR: ' + str(exception) })
            return "ER0"
    else:
        print("DOWNLOAD DATABASE: Successfully downloaded taxonomy files")


    # Create seqid2taxid.map file

    seqid2taxid_file = open(app_location_database + 'seqid2taxid.map','a+')
    seqid2taxid_file.write('\n')

    print("DOWNLOAD DATABASE: Created seqid2taxid.map file")
    self.update_state(state="PROGRESS", meta={'percent-done': 20, 'message': 'Creating seqid2taxid.map file'})

    tmp = "NO_OUTPUT"

    if len(queries) == 0:
        print("DOWNLOAD DATABASE: No queries provided, skipping.")
    else:
        print("DOWNLOAD DATABASE: Entering queries into taxonomy files.")
        self.update_state(state="PROGRESS", meta={'percent-done': 30, 'message': 'Entering queries into taxonomy files.'})
        for query in queries:
            query_file = open(query['file'],'r')

            # Generate non-existant tax_id from the given parent tax_id
            proposed_tax_id = ''
            already_seen_tax_id = []
            while True:
                randNum = random.randint(100,999)
                proposed_tax_id = query['parent'] + str(randNum)
                record = Entrez.read(Entrez.efetch(db='Taxonomy',id=proposed_tax_id,retmode='xml'))

                if len(record) == 0 and proposed_tax_id not in already_seen_tax_id:
                    already_seen_tax_id.append(proposed_tax_id)
                    break

            NCBI_id = "sequence_" + str(proposed_tax_id) # Sequence ID for seqid2taxid.map file

            # Update alertconfig file with the new proposed tax_id.
            import ast
            other_info = ""
            list_of_sequences = ""
            line_number = -1
            with open(app_location + 'alertinfo.cfg','r') as read_file:
                for idx,line in enumerate(read_file):
                    if "alert_sequences" not in line:
                        other_info += line
                    else:
                        list_of_sequences = line.split("=")[1]

            list_of_sequences = ast.literal_eval(list_of_sequences.strip())
            list_of_sequences.append(str(proposed_tax_id))
            new_list_of_sequences = "alert_sequences = " + str(list_of_sequences)
            with open(app_location + 'alertinfo.cfg','w') as write_file:
                write_file.write(other_info)
                write_file.write(new_list_of_sequences)


            # Add an entry in seqid2taxid with NCBI's id and the tax_id
            print("DOWNLOAD_DATABASE: Added " + query['name'] + " entry in seqid2taxid.map file")
            self.update_state(state="PROGRESS", meta={'percent-done': 35, 'message': "Added " + query['name'] + " entry in seqid2taxid.map file"})
            seqid2taxid_file.write(str(NCBI_id) + '\t' + str(proposed_tax_id) + '\n')

            # Add an entry in nodes.dmp with parent tax_id, tax_id and rank (species)
            entry = str(proposed_tax_id) + '\t|\t' + str(query['parent']) + '\t|\t' + 'species\t|\n'
            with open(app_location_database + 'taxonomy/nodes.dmp','a+') as nodes_dmp:
                nodes_dmp.write(entry)
            print("DOWNLOAD_DATABASE: Added " + query['name'] + " entry in nodes.map file")
            self.update_state(state="PROGRESS", meta={'percent-done': 40, 'message': "Added " + query['name'] + " entry in nodes.dmp file"})

            # Add an entry in names.dmp with tax_id, sci_name, empty unique name, name_class (scientific name)
            entry = str(proposed_tax_id) + '\t|\t' + str(query['name']) + '\t|\t \t|\t' + 'scientific name \t|\n'
            with open(app_location_database + 'taxonomy/names.dmp','a+') as names_dmp:
                names_dmp.write(entry)
            print("DOWNLOAD_DATABASE: Added " + query['name'] + " entry in names.map file")
            self.update_state(state="PROGRESS", meta={'percent-done': 45, 'message': "Added " + query['name'] + " entry in names.dmp file"})

            # Putting all the query sequences in one, input_sequences file.
            with open(app_location_database + 'input_sequences.fa','a+') as input_sequences:
                input_sequences.write('\n')
            with open(query['file'], 'rb') as query_file, open(app_location_database + 'input_sequences.fa','ab+') as input_sequences:
                shutil.copyfileobj(query_file, input_sequences)
            print("DOWNLOAD_DATABASE: Merged " + query['file'] + " sequence into input_sequences.fa file.")

            self.update_state(state="PROGRESS", meta={'percent-done': 50, 'message': "Merged " + query['file'] + " sequence into input_sequences.fa file."})





    # If user has requested to download NCBI's database
    if bacteria == True or archaea == True or virus == True:

        # Construct db_string first
        db_list = []
        if bacteria == True:
            db_list.append('bacteria')
        if archaea == True:
            db_list.append('archaea')
        if virus == True:
            db_list.append('viral')

        db_string = ",".join([str(x) for x in db_list])

        print("DOWNLOAD_DATABASE: Downloading " + db_string + " database(s) from NCBI")
        self.update_state(state="PROGRESS", meta={'percent-done': 55, 'message': "Downloading " + db_string + " database(s) from NCBI"})
        cmd = ['centrifuge-download -o ' + app_location_database + 'library -m -d ' + db_string + ' refseq']
        print("CMD: ")
        print(cmd)
        outfile = open(app_location_database + 'seqid2taxid.map','a+')
        errfile = open(app_location_database + 'download_err.txt','w+')
        try:
            # Download the database.
            download_bacteria_output = subprocess.Popen(
                cmd,
                shell=True,
                stdout=outfile,
                stderr=errfile
            )
            download_bacteria_output.communicate()
            download_bacteria_output.wait()

        except (OSError, subprocess.CalledProcessError) as exception:
            print(str(exception))
            return "ER1"

        else:
            print("DOWNLOAD_DATABASE: Successfully downloaded " + db_string + " database(s) from NCBI")
            self.update_state(state="PROGRESS", meta={'percent-done': 90, 'message': "Successfully downloaded " + db_string + " database(s) from NCBI"})

            import glob

            # Putting all the query sequences in one, input_sequences file.
            print("DOWNLOAD_DATABASE: Concatinating all the sequnce files.")
            self.update_state(state="PROGRESS", meta={'percent-done': 95, 'message': "Concatinating all the sequnce files."})
            with open(app_location_database + 'input_sequences.fa', 'wb') as outfile:
                for filename in glob.glob(app_location_database + 'library/*/*.fna'):
                    with open(filename, 'rb') as readfile:
                        shutil.copyfileobj(readfile, outfile)

    # Generate database index.
    import datetime
    now = datetime.datetime.now()

    print("DOWNLOAD_DATABASE: Building the index.")
    self.update_state(state="PROGRESS", meta={'percent-done': 98, 'message': "Building the index."})
    index_cmd = ['centrifuge-build --conversion-table ' + app_location_database + \
           'seqid2taxid.map --taxonomy-tree ' + app_location_database + \
           'taxonomy/nodes.dmp --name-table ' + app_location_database + \
           'taxonomy/names.dmp ' + app_location_database + 'input_sequences.fa' + \
           ' ' + str(now.year) + str(now.month) + str(now.day) + str(now.hour) + \
           str(now.minute) + str(now.second)
          ]
    print("IDX CMD: ")
    print(index_cmd)
    building_index_output = open(app_location_database + 'building_index.txt','w+')
    try:
        # Download the database.
        download_bacteria_output = subprocess.Popen(
            index_cmd,
            shell=True,
            stdout=building_index_output,
            stderr=building_index_output
        )
        download_bacteria_output.communicate()
        download_bacteria_output.wait()

    except (OSError, subprocess.CalledProcessError) as exception:
        print(str(exception))
        return "ER1"

    else:
        print("DOWNLOAD_DATABASE: Database has successfully been downloaded and built.")
        self.update_state(state="PROGRESS", meta={'percent-done': 100, 'message': "Database has successfully been downloaded and built."})


    return tmp
