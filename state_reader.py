import pprint
import ezgmail

import io
from PIL import Image
import pytesseract
from wand.image import Image as wi

import shutil
import os
from os.path import isfile, join

import time

import config


def is_crypto(_filename):
	pdf = wi(filename=_filename, resolution=140)
	pdf_image = pdf.convert('jpeg')

	image_blobs = []

	for img in pdf_image.sequence:
		img_page = wi(image=img)
		image_blobs.append(img_page.make_blob('jpeg'))

	recognized_text = []

	for imgBlob in image_blobs:
		im = Image.open(io.BytesIO(imgBlob))
		text = pytesseract.image_to_string(im, lang='eng')
		recognized_text.append(text)

	keyword = "crypto"

	for i in recognized_text:
		# print(i)
		if keyword in i:
			print("CRYPTO STATEMENT!!!")
			return True

	print("PRAESCIRE STATEMENT")
	return False


def delete_cache():
	tempdir = '/tmp'
	files = os.listdir(tempdir)
	for file in files:
		if "magick" in file:
			os.remove(os.path.join(tempdir, file))
	print("Removed temporary files")


def ocr_classification():
	statements_path = config.SETTINGS['statements_path']
	files = os.listdir(statements_path)
	# files = [f for f in os.listdir(statements_path) if os.path.isfile(f)]
	print(files)

	newlist = []
	for names in files:
		if names.endswith(".pdf"):
			newlist.append(names)
	print(newlist)

	delete_cache()

	count = 0
	for pdf in newlist:
		base_path = statements_path
		crypto_store = config.SETTINGS['crypto_store']
		pdf_path = base_path + pdf
		print(pdf_path)

		if is_crypto(pdf_path):
			shutil.move(pdf_path, crypto_store + pdf)

		# every 3 files we will clear cache and pause 20 seconds and pause for a minute after 10 files
		if count % 3 == 0:
			print("count is ", count, ": CLEARING CACHE")
			time.sleep(20)
			delete_cache()
			if count != 0 and count % 10 == 0:
				print("Snoozzze for 1 minute")
				time.sleep(60)

		count += 1
	print("completed moving the files around")

	delete_cache()


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

		for month in months:
			if month in file:
				monthly_checker(month, statements_path, old_file_path)
				break


def monthly_checker(month, statements_path, old_file_path):
	print("Moving to", month, "folder")
	path = os.path.join(statements_path, month)
	filename = os.path.basename(old_file_path)

	if not os.path.exists(path):
		print("Folder does not exist so creating")
		os.mkdir(path)
		shutil.move(old_file_path, path)
	else:
		# 				folder exists so just copy it over:

		print(filename, "Value of file checking", os.path.isfile(os.path.join(path, filename)))
		if not os.path.isfile(os.path.join(path, filename)):
			new_file_name = os.path.join(path, filename)
			shutil.move(old_file_path, new_file_name)
		else:
			print("no moving, filename already exists")


def run():
	max_emails = 100
	ezgmail.init()
	print(ezgmail.EMAIL_ADDRESS)

	print("Searching for ig statement attachments in 2020, unread ")
	email_threads = ezgmail.search("2020 from:'statements@ig.com' label:unread has:attachment", maxResults=max_emails)

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

			item.downloadAttachment(filename, config.SETTINGS['local_tmp'], duplicateIndex=0)
			# MOVE ITEM INTO statements_folder
			move()

		count += 1

	# move files to google drive:
	# we use dropbox as it works better in ubuntu!
	base_dir = config.SETTINGS['statements_path']

	move(movdir=config.SETTINGS['local_statements'], basedir=base_dir)

	ezgmail.markAsRead(email_threads)

	# Classify the files in dropbox and move them across
	ocr_classification()

	# move into folders
	mov_into_monthly(config.SETTINGS['statements_path'])


if __name__ == '__main__':
	run()
