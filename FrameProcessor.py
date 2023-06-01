# Redhwan Ahmed
# Frame Processor

import os
import pymongo
import csv
import argparse
import datetime
import xlsxwriter as xls
import ffmpeg
import math

# Project 2, Step 1: Argparse Arguments
#output = []
parser = argparse.ArgumentParser(description='Handoff arguments')
parser.add_argument('--files', help='Baselight/Flames Text files')
parser.add_argument('--xytech', help='Xytech file input')
parser.add_argument('--verbose', action='store_true', help='Console output on/off')
parser.add_argument('--process', help='Process video')
parser.add_argument('--output', choices=['csv', 'database', 'db', 'xls'], help='Output to CSV, Database, or Excel')
args = parser.parse_args()


# Step 2: MongoDB Connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["proj2chaja"]
mycol = mydb["currentuser"]
mycol_two = mydb["location"]

def file_parser(xytech_file, baselight_file, flame):
    current_user = os.environ.get('USERNAME') or os.environ.get('USER')
    output = []
    #Open Xytech file
    xytech_file_location = xytech_file
    xytech_folders = []
    read_xytech_file = open(xytech_file_location, "r")
    xytech_lines = read_xytech_file.readlines()
    #Insert Workorder, Producer, Operator, and Job into output from Xytech file

    # Extract the required lines
    workorder_line = xytech_lines[0].strip()
    producer_line = xytech_lines[1].strip()
    operator_line = xytech_lines[2].strip()
    job_line = xytech_lines[3].strip()
    notes_line = xytech_lines[-1].strip()

    # Extract the required fields from the lines
    workorder = workorder_line.split(' ')[-1]
    producer = producer_line.split(': ')[-1]
    operator = operator_line.split(': ')[-1]
    job = job_line.split(': ')[-1]
    notes = notes_line.split(': ')[-1]

    # Construct the output string
    output_string = f'{current_user},Xytech Workorder {workorder}{producer},{operator},{job},{notes}'
    output.append(output_string)

    for line in read_xytech_file:
        if "/" in line:
            xytech_folders.append(line)

    #Open Baselight file
    baselight_file_location = baselight_file
    read_baselight_file = open(baselight_file_location, "r")

    #Read each line from Baselight file
    for line in read_baselight_file:
        line_parse = line.split(" ")
        if flame:
            current_folder = line_parse.pop(1)
        else:
            current_folder = line_parse.pop(0)
        sub_folder = current_folder.replace("/images1/starwars", "")
        new_location = ""

        #Folder replace check
        for xytech_line in xytech_folders:
            if sub_folder in xytech_line:
                new_location = xytech_line.strip()
        first = None
        last = None
        for numeral in line_parse:
            number_string = "";
            if not numeral.strip().isnumeric():
                #Skip <err> and <null>
                continue

            if first is None:
                first = int(numeral)
                last = first
            elif int(numeral) == last + 1:
                last = int(numeral)
            else:
                # Range ends, output
                if first == last:
                    if flame:
                        number_string = ("%s %s %s" % ("/net/flame-archive", sub_folder, first))
                    else:
                        number_string = ("%s %s" % (sub_folder, first))
                    output.append(number_string)
                    #print(sub_folder +" "+ number_string)
                else:
                    if flame:
                        number_string = ("%s %s %s-%s" % ("/net/flame-archive", sub_folder, first, last))
                    else:
                        number_string = ("%s %s-%s" % (sub_folder, first, last))
                    output.append(number_string)
                    #print(sub_folder +" "+ number_string)
                first = int(numeral)
                last = first

        # Working with last number each line
        if first is not None:
            if first == last:
                if flame:
                    number_string = ("%s %s %s" % ("/net/flame-archive", sub_folder, first))
                else:
                    number_string = ("%s %s" % (sub_folder, first))
                output.append(number_string)
                #print(sub_folder +" "+ number_string)
            else:
                if flame:
                    number_string = ("%s %s %s-%s" % ("/net/flame-archive", sub_folder, first, last))
                else:
                    number_string = ("%s %s-%s" % (sub_folder, first, last))
                output.append(number_string)
                #print(sub_folder +" "+ number_string)
    return output

#frame_counter
def frame_counter(video_file):
    probe = ffmpeg.probe(video_file)
    video_info = next(stream for stream in probe['streams'] if stream['codec_type'] == 'video')
    frame_count = int(video_info['nb_frames'])
    return frame_count

#thumbnail_maker
def thumbnail_maker(video_file, frame_number):
    thumbnail_file = f"thumbnails/thumbnail_{frame_number}.jpg"
    (
        ffmpeg.input(video_file)
        .filter("select", f"gte(n,{frame_number})")
        .output(thumbnail_file, vframes=1, s="96x74")
        .run()
    )
    return thumbnail_file



# proj3 addition: (does the video processing)
if not args.process:
    # proj2 code: (does the function calls and data base stuff)
    baselight_files = args.files.split(" ")
    xytech_file = args.xytech
    i = 0; # Counter for file names
    # Parse the files using file_parser
    for file in baselight_files:
        if "Flame" in file:
            output = file_parser(xytech_file, file, True)
        else:
            output = file_parser(xytech_file, file, False)

        # Get Machine, User, and Date from file name
        # Get current user from output
        current_user = output[0].split(",")[0];
        # Get Machine name from file
        machine_name = file.split("_")[0];
        # Get user name from file
        user_name = file.split("_")[1];
        # Get date from file
        date = file.split("_")[2].split(".")[0];

        # Check if verbose is on, if so print the output
        if args.verbose:
            for line in output:
                print(line)

        # Check if output is csv, if so write to csv file, if not write to database.
        if args.output == "csv":
            with open(f'output{i}.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                for line in output:
                    writer.writerow([line])
        elif args.output == "db" or args.output == "database":
            current_user_db = {
                "current_user": current_user,
                "machine_name": machine_name,
                "user_name": user_name,
                "date": date,
                "submitted_date": datetime.datetime.now()
            }
            mycol.insert_one(current_user_db)
            
            for line in output:
                if "/" in line:
                    if "Flame" in file:
                        storage_location = "/net/flame-archive"
                        location = line.split(" ")[1]
                        frame_range = line.split(" ")[2]
                        frame_range_db_insert = {
                            "machine_name": machine_name,
                            "user_name": user_name,
                            "date": date,
                            "storage_location": storage_location,
                            "location": location,
                            "frame_range": frame_range
                        }
                    else:
                        storage_location = "NA"
                        location = line.split(" ")[0]
                        frame_range = line.split(" ")[1]
                        frame_range_db_insert = {
                            "machine_name": machine_name,
                            "user_name": user_name,
                            "date": date,
                            "storage_location": storage_location,
                            "location": location,
                            "frame_range": frame_range
                        }
                    mycol_two.insert_one(frame_range_db_insert)
        i+=1 # Increment i for output file name
else:
    # PROJ 3 CODE:
    # This is for processing the video, making the database calls from the last project, sorting the entries from the database, and then doing the processing for project3.

    # Initialization: Start an excel file for the output
    workbook = xls.Workbook('output.xlsx')
    worksheet = workbook.add_worksheet()
    # Set the columns to resemble the DB entry format
    worksheet.write('A1', 'Machine Name')
    worksheet.write('B1', 'User Name')
    worksheet.write('C1', 'Date')
    worksheet.write('D1', 'Storage Location')
    worksheet.write('E1', 'Location')
    worksheet.write('F1', 'Frame Range')
    worksheet.write('G1', 'Thumbnail')

    # Step 1: Count the number of frames in the video (passed in by args.process)
    frame_count = frame_counter(args.process)

    # Step 2: Get the frame ranges from the database
    frame_ranges = mycol_two.find()

    # Step 3: Create a list of entries from the database that are in the frame range before the frame count
    frame_range_list = []
    for frame_range in frame_ranges:
        if int(frame_range["frame_range"].split("-")[0]) <= frame_count:
            frame_range_list.append(frame_range)

    # Step 4: Rreplace the frame_range attribute in the list with the average of the frame range
    for frame_range in frame_range_list:
        if "-" in frame_range["frame_range"]:
            frame_range["frame_range"] = math.floor((int(frame_range["frame_range"].split("-")[0]) + int(frame_range["frame_range"].split("-")[1])) / 2)
            # drop the decimal point if there is one

    # Step 5: Sort the list by frame range
    frame_range_list.sort(key=lambda x: int(x["frame_range"]))

    # Step 6: Using the frame_range attriubute from frame_range_list, create a list of thumbnail locations
    thumbnail_list = []
    for frame_range in frame_range_list:
        thumbnail_list.append(thumbnail_maker(args.process, frame_range["frame_range"]))
    
    # Step 7: Write the data to the excel file
    i = 1
    for frame_range in frame_range_list:
        worksheet.write(f'A{i+1}', frame_range["machine_name"])
        worksheet.write(f'B{i+1}', frame_range["user_name"])
        worksheet.write(f'C{i+1}', frame_range["date"])
        worksheet.write(f'D{i+1}', frame_range["storage_location"])
        worksheet.write(f'E{i+1}', frame_range["location"])
        worksheet.write(f'F{i+1}', frame_range["frame_range"])
        worksheet.insert_image(f'G{i+1}', thumbnail_list[i-1])
        i+=1
    # Step 8: Make the excel file look nice
    center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
    worksheet.set_column('A:G', 30, center)
    # Step 9: Make all the rows 74 pixels tall
    worksheet.set_default_row(74)

    # Step 8: Close the workbook
    workbook.close()