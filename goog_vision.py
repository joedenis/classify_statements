"""
Statement download and classification in Google Cloud
"""


import glob
import os
from os.path import isfile, join
import re
import shutil
import time
from google.cloud import storage
import config
import ezgmail

# setting the payment details for our vision and storage account!
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/joe/PycharmProjects/ezgmail_statements/vision_credentials.json"

betaVersion = False


def implicit():
    """Used to test if cloud is set up correctly"""
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))


def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """Copies a blob from one bucket to another with a new name.
    Needed for moving crypto files around and also for the other files
    """
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)

    print('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name))


def delete_blob(bucket_name, blob_name):
    """
    Deletes a blob from the bucket.
    Needed after moving a file around!
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.delete()

    print('Blob {} deleted.'.format(blob_name))


def async_detect_document(gcs_source_uri, gcs_destination_uri):
    """OCR with PDF/TIFF as source files on GCS
    returns True False (is crypto statement)
    """
    import json
    from google.cloud import vision
    from google.cloud import storage
    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'

    # How many pages should be grouped into each json output file.
    batch_size = 2

    client = vision.ImageAnnotatorClient()

    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print('Waiting for the operation to finish.')
    operation.result(timeout=180)

    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.
    storage_client = storage.Client()

    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    # List objects with the given prefix.
    blob_list = list(bucket.list_blobs(prefix=prefix))
    # print('Output files:')
    # for blob in blob_list:
    #     print(blob.name)

    # Process the first output file from GCS.
    # Since we specified batch_size=2, the first response contains
    # the first two pages of the input file.
    output = blob_list[0]

    json_string = output.download_as_string()
    response = json.loads(json_string)

    # The actual response for the first page of the input file.
    first_page_response = response['responses'][0]
    annotation = first_page_response['fullTextAnnotation']
    # Here we print the full text from the first page.
    # The response contains more information:
    # annotation/pages/blocks/paragraphs/words/symbols
    # including confidence scores and bounding boxes
    # print(u'Full text:\n{}'.format(annotation.text))

    if "Account Name Spread bet" in annotation['text']:
        print("PRAESCIRE STATEMEMT")
        return False
    else:
        print("CRYPTO STATEMENT")

    return True


def list_blobs(bucket_name, _prefix=None):
    """returns list of strings of all the blobs in the bucket that contain .pdf"""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=_prefix)

    blob_pdf_strings = []
    for blob in blobs:
        if '.pdf' in blob.name:
            # print(blob.name)
            blob_pdf_strings.append(blob.name)

    return blob_pdf_strings


def file_type_blobs(blobs, extension):
    """

    :param blobs:
    :param extension:
    :return: list of file paths from the blobs with a given extension (.pdf for example)
    """
    out = []
    for blob in blobs:
        if extension in blob.name:
            out.append(blob)

    print(out)
    return out


def move_and_delete(bucket_name, blob_list, destination):
    """
    downloads all the files in a blob to a destination
    :param bucket_name:
    :param blob_list:
    :param destination: dropbox we use
    :return:
    """
    for path in blob_list:
        file_name = os.path.basename(path)
        destination_path = destination + file_name
        download_blob(bucket_name, path, destination_path)
        delete_blob(bucket_name, path)


def mov_into_monthly(statements_path):
    """
    have filenames containing months:
    Jan Feb Mar Apr May Jun Jul Aug Sep Nov Dec
    if file name contains monthly name move to the appropriate folder
    """
    onlyfiles = [f for f in os.listdir(statements_path) if isfile(join(statements_path, f))]
    print(onlyfiles)
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    for file in onlyfiles:
        old_file_path = os.path.join(statements_path, file)
        if "Share" in file or "ISA" in file:
            isa_dir = statements_path + "ISAs/"
            if not os.path.isfile(os.path.join(isa_dir, file)):
                shutil.move(old_file_path, os.path.join(isa_dir, file))
                print("Moved", file, "into ISA folder")
            else:
                print("ALREADY EXISTS:", file)
                print("deleting: ", old_file_path)
                os.remove(old_file_path)
        else:
            for month in months:
                if month in file:
                    monthly_checker(month, statements_path, old_file_path)
                    break


def city_mov_monthly(statements_path):
    """have filenames containing year month day time
    2021 11 30 220000
    2021 11 30 2200000
    """
    onlyfiles = [f for f in os.listdir(statements_path) if isfile(join(statements_path, f))]
    print(onlyfiles)

    months = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05':'May',
               '06': 'Jun', '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}

    for file in onlyfiles:
        # we only want to move cityIndex files.  They have more than 10 digits in the filename
        digitsinFile = sum(c.isdigit() for c in file)
        if digitsinFile > 12:

            old_file_path = os.path.join(statements_path, file)

            month_digit = file[4:6]
            monthly_checker(months[month_digit], statements_path, old_file_path)


def monthly_checker(month, statements_path, old_file_path):
    print("Moving to", month, "folder")
    path = os.path.join(statements_path, month)
    filename = os.path.basename(old_file_path)

    if not os.path.exists(path):
        print("Folder does not exist so creating")
        os.mkdir(path)
        shutil.move(old_file_path, path)
    else:
        # folder exists so just sopy it over
        print(filename, "Value of file checking", os.path.isfile(os.path.join(path, filename)))
        if not os.path.isfile(os.path.join(path, filename)):
            new_file_name = os.path.join(path, filename)
            shutil.move(old_file_path, new_file_name)
        else:
            print("no moving, filename already exists")
            os.remove(old_file_path)
            print("Deleted:", old_file_path)


def move(movdir=config.SETTINGS['local_tmp'], basedir=config.SETTINGS['local_statements']):
    # Walk through all files in the directory that contains the files to copy
    for root, dirs, files in os.walk(movdir):
        for filename in files:
            # I use absolute path, case you want to move several dirs.
            old_name = os.path.join(os.path.abspath(root), filename)

            # Separate base from extension
            base, extension = os.path.splitext(filename)

            # Initial new name
            new_name = os.path.join(basedir, filename)
            # If folder basedir/base does not exist... You don't want to create it?
            # if not os.path.exists(os.path.join(basedir, base)):
            if not os.path.exists(basedir):
                print(os.path.join(basedir, base), "not found")
                continue  # Next filename
            elif not os.path.exists(new_name):  # folder exists, file does not
                # shutil.copy(old_name, new_name)
                shutil.move(old_name, new_name)
            else:  # folder exists, file exists as well
                ii = 1
                while True:
                    new_name = os.path.join(basedir, base + "_" + str(ii) + extension)
                    if not os.path.exists(new_name):
                        shutil.move(old_name, new_name)
                        print("Copied", old_name, "as", new_name)
                        break
                    ii += 1


def attachment_downloads(year, tag="from:'no-reply.statements@ig.com' label:unread has:attachment"):
    """
    downloads our gmail attachments into a local directory
    :return:
    """
    max_emails = 100
    ezgmail.init()
    print(ezgmail.EMAIL_ADDRESS)

    print("Searching for ig statement attachments in", str(year), "unread")
    search_term = str(year) + " " + tag
    email_threads = ezgmail.search(
        search_term,
        maxResults=max_emails
    )

    # threads = ezgmail.search("2011 from:'statements@igindex.co.uk' has:attachment", maxResults=MAX_RESULTS)

    print(email_threads)
    print(len(email_threads))

    print("iterating through all the threads")
    count = 1
    for thread in email_threads:

        print("email thread", count, ":", thread)
        file = thread.messages
        for item in file:
            file = item.attachments
            # attachment_name = pprint.pprint(file)
            print("printing how the attachment reads", file)

            filename = file[0]
            
            if not "segregation" in filename:
                item.downloadAttachment(filename, config.SETTINGS['local_tmp'], duplicateIndex=0)
                # MOVE ITEM INTO statements_folder
                move()

        count += 1

    ezgmail.markAsRead(email_threads)


def copy_local_directory_to_gcs(local_path, bucket_name, gcs_path):
    """Recursively copy a directory of files to GCS.

    local_path should be a directory and not have a trailing slash.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    assert os.path.isdir(local_path)
    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file):
            continue
        remote_path = os.path.join(gcs_path, local_file[1 + len(local_path):])
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_file)


def main(year):
    """
    Downloads pdf attachments from gmail
    Moves all of the pdf statements stored in 'pdf_statements' into either crypto or praescire_statement folders
    Works via classification in the cloud.
    3 pages per statement.
    Then moves all statements from Google Cloud into Dropbox so we can view the files locally
    """

    attachment_downloads(year)

    # SOURCE = "gs://praescire_statements/statement_crypto.pdf"
    SOURCE = "gs://praescire_statements/"
    BUCKET = "praescire_statements"
    OUTPUT_PREFIX = 'OCR_PDF_TEST_OUTPUT'

    GCS_DESTINATION_URI = 'gs://{}/{}/'.format(BUCKET, OUTPUT_PREFIX)

    # GCS_CRYPTO_URI = 'gs://{}/{}/'.format(BUCKET, CRYPTO_PREFIX)
    # async_detect_document(SOURCE, GCS_DESTINATION_URI)

    copy_local_directory_to_gcs(config.SETTINGS['local_statements'], BUCKET, 'pdf_statements/')

    # delete the local files as they are now in the cloud
    filelist = glob.glob(os.path.join(config.SETTINGS['local_statements'], "*.pdf"))
    for f in filelist:
        os.remove(f)

    """
    Here we are getting all the pdfs in the statements folder and then running ocr
    """
    storage_client = storage.Client()
    buckets = list(storage_client.list_buckets())
    print(buckets)

    pdf_blobs_str = list_blobs(BUCKET, _prefix='pdf_statements/')

    print(pdf_blobs_str)

    for statement_path in pdf_blobs_str:
        path_to_pdf = SOURCE + statement_path
        print(path_to_pdf)

    #     do the clasification on the blob_path
        is_crypto = async_detect_document(path_to_pdf, GCS_DESTINATION_URI)
        if is_crypto:
            # replace the path_to_pdf in the string to move folders easily
            new_path = statement_path.replace("pdf_statements/", "crypto/")
        else:
            new_path = statement_path.replace("pdf_statements/", "praescire_statements/")
        # copy the newl classified statement to the praescire_statement folder
        copy_blob(BUCKET, statement_path, BUCKET, new_blob_name=new_path)
        delete_blob(BUCKET, statement_path)

    # moving the files from google cloud to dropbox
    crypto_blob_str = list_blobs(BUCKET, _prefix='crypto/')
    crypto_store = config.SETTINGS['crypto_store']
    move_and_delete(BUCKET, crypto_blob_str, crypto_store)

    praescire_blob_str = list_blobs(BUCKET, _prefix='praescire_statements/')

    if betaVersion:
        statements_store = config.SETTINGS['statements_path'] + 'google_vision/'
    else:
        statements_store = config.SETTINGS['statements_path']

    move_and_delete(BUCKET, praescire_blob_str, statements_store)

    # performing monthly folders in dropbox for the praescire_statements
    mov_into_monthly(statements_store)


def city_statements(year):
    """
    All city statements are for praescire so we just need to move them into the correct folder.
    No OCR

    year = 2021
    """
    attachment_downloads(year, tag="from:'Statements@cityindex.com' label:unread has:attachment")

    SOURCE = "gs://praescire_statements/"
    BUCKET = "praescire_statements"

    copy_local_directory_to_gcs(config.SETTINGS['local_statements'], BUCKET, 'pdf_statements/')

    # delete the local files as they are now in the cloud
    filelist = glob.glob(os.path.join(config.SETTINGS['local_statements'], "*.pdf"))
    for f in filelist:
        os.remove(f)

    """
    Here we are getting all the pdfs in the statements folder
    """
    storage_client = storage.Client()
    buckets = list(storage_client.list_buckets())
    print(buckets)

    pdf_blobs_str = list_blobs(BUCKET, _prefix='pdf_statements/')

    print(pdf_blobs_str)

    for statement_path in pdf_blobs_str:
        path_to_pdf = SOURCE + statement_path
        print(path_to_pdf)

        # cityIndex statements are all praescire statements
        new_path = statement_path.replace("pdf_statements/", "praescire_statements/")
        # copy to the praescire_statement folder
        copy_blob(BUCKET, statement_path, BUCKET, new_blob_name=new_path)
        delete_blob(BUCKET, statement_path)

    # moving the files from google cloud to dropbox
    praescire_blob_str = list_blobs(BUCKET, _prefix='praescire_statements/')

    if betaVersion:
        statements_store = config.SETTINGS['statements_path'] + 'google_vision/'
    else:
        statements_store = config.SETTINGS['statements_path']

    move_and_delete(BUCKET, praescire_blob_str, statements_store)
    city_mov_monthly(statements_store)


if __name__ == "__main__":

    start_time = time.time()
    year = 2022
    main(year)
    city_statements(year)

    print("time elapsed: {:.2f}s".format(time.time() - start_time))
