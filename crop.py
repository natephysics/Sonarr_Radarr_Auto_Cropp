import json
import subprocess
import csv
import os
import argparse
import pandas as pd
from sys import exit
import logging

# Path to CSV with manual crop data. Should contain the following columns: "Series", "Season", "Horizontal", "Vertical"
TV_file_path = "E:\Autocrop\TVDB.csv"
Movie_file_path = "E:\Autocrop\MoviesDB.csv"

# Folder to store the logfiles.
log_path = "E:\Autocrop\logs\TV"
# Logging level (info, debug)
logging_level = logging.INFO

# Sometimes the automatic crop values are not correct for videos that don't need cropping at all.
# By providing a tolerance we can prevent these edge cases from being cropped to odd resolutions.
# rel_tol=10 means that the difference between the two resolutions is less than 10 pixels
rel_tol = 18

# The enviromeent variables that are available when sonarr/radarr adds or upgrades a file
variables = [
    "sonarr_eventtype",
    "sonarr_isupgrade",
    "sonarr_series_id",
    "sonarr_series_title",
    "sonarr_episodefile_path",
    "sonarr_series_tvdbid",
    "sonarr_series_tvmazeid",
    "sonarr_series_imdbid",
    "sonarr_series_type",
    "sonarr_episodefile_id",
    "sonarr_episodefile_episodecount",
    "sonarr_episodefile_relativepath",
    "sonarr_episodefile_path",
    "sonarr_episodefile_episodeids",
    "sonarr_episodefile_seasonnumber",
    "sonarr_episodefile_episodenumbers",
    "sonarr_episodefile_episodeairdates",
    "sonarr_episodefile_episodeairdatesutc",
    "sonarr_episodefile_episodetitles",
    "sonarr_episodefile_quality",
    "sonarr_episodefile_qualityversion",
    "sonarr_episodefile_releasegroup",
    "sonarr_episodefile_scenename",
    "sonarr_episodefile_sourcepath",
    "sonarr_episodefile_sourcefolder",
    "radarr_eventtype",
    "radarr_download_id",
    "radarr_download_client",
    "radarr_isupgrade",
    "radarr_movie_id",
    "radarr_movie_imdbid",
    "radarr_movie_in_cinemas_date",
    "radarr_movie_path",
    "radarr_movie_physical_release_date",
    "radarr_movie_title",
    "radarr_movie_tmdbid",
    "radarr_movie_year",
    "radarr_moviefile_id",
    "radarr_moviefile_relativepath",
    "radarr_moviefile_path",
    "radarr_moviefile_quality",
    "radarr_moviefile_qualityversion",
    "radarr_moviefile_releasegroup",
    "radarr_moviefile_scenename",
    "radarr_moviefile_sourcepath",
    "radarr_moviefile_sourcefolder",
    "radarr_deletedrelativepaths",
    "radarr_deletedpath",
]


def get_logger(log_name):
    """logger configuration helper."""
    log_file = f"{log_path}/crop_{log_name}.log"
    log_format = "%(asctime)s %(levelname)-10s %(message)s"
    log_date_format = "%y/%m/%d %H:%M:%S"
    formatter = logging.Formatter(log_format, log_date_format)
    fhandler = logging.FileHandler(log_file)
    fhandler.setFormatter(formatter)
    logger = logging.getLogger(log_name)
    if not len(logger.handlers):
        logger.addHandler(fhandler)
    logger.setLevel(logging_level)
    logger.propagate = False
    return logger


# The error and info will be sent to different files, just to make things more simple to read and find
error_log = get_logger("err")
info_log = get_logger("info")


def read_csv(csv_file):
    with open(csv_file, "r") as file:
        csv_dict = [
            {k: v for k, v in row.items()}
            for row in csv.DictReader(file, delimiter=",", skipinitialspace=True)
        ]
    return csv_dict


def crop_video(source_path, destination_path, width, height):
    # Since the FFMPEG operations is "in-place" it will overwrite the original file, which is not what we want.
    # Since the original file is hardlinked it's safe for us to delete it first and then create a new encode with the same name.
    # This way we can keep the original file and the new file will have the same name.

    # Delete the original file
    cmd = ["powershell", "-Command", f'del "{destination_path}"']
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    errorcode = process.returncode
    if errorcode > 0:
        error_log.warning(f"Failed to delete existing file: {cmd}")
        exit(1)

    # Crop the video using the source path as the input and the destination path as the output.
    # Prepare the crop command
    crop_cmd = [
        "ffmpeg",
        "-y",
        "-i",
        source_path,
        "-filter:v",
        f"crop={width}:{height}",
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-tune",
        "film",
        "-profile:v",
        "high",
        "-level",
        "4.1",
        "-crf",
        "19",
        "-c:a",
        "copy",
        "-c:s",
        "copy",
        "-map",
        "0",
        "-max_muxing_queue_size",
        "1024",
        "-threads",
        "4",
        destination_path,
    ]
    crop_video_process = subprocess.run(crop_cmd, shell=True)
    errcode = crop_video_process.returncode
    if errcode > 0:
        error_log.warning(
            f"cmd failed during crop_video, see above for details. Command: {crop_cmd}"
        )
        exit(1)
    else:
        info_log.info(
            f"Video {destination_path} successfully cropped to {width}:{height}"
        )


def check_video_resolution(video_path, width, height):
    # check to see the current resolution of the video.
    cmd = f'ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of json "{video_path}"'
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    errorcode = process.returncode
    if errorcode > 0:
        error_log.warning(
            "cmd failed during check_video_resolution, see above for details. Command: ",
            cmd,
        )
        exit(1)

    # Parse the output of the ffprobe command
    resolution = json.loads(process.stdout)
    current_width = resolution["streams"][0]["width"]
    current_height = resolution["streams"][0]["height"]
    info_log.debug(
        f"Video {video_path} has resolution {current_width}x{current_height} and should be {width}x{height} within a tolerance of ±{rel_tol}."
    )

    # Check to see if the current resolution is within the tolerance of the desired resolution
    if (
        abs(int(width) - current_width) < rel_tol
        and abs(int(height) - current_height) < rel_tol
    ):
        return True
    else:
        return False


def get_crop_parameters(video_path):
    # First the video_path is converted to a path that bash can understand, using the wslpath utility, which is native
    convert_path_to_bash_cmd = f"bash -c \"wslpath -a '{video_path}'\""
    bash_path = (
        subprocess.run(convert_path_to_bash_cmd, shell=True, stdout=subprocess.PIPE)
        .stdout.decode()
        .strip("\n")
    )

    # The following command will take a 5min clip starting from 2mins into the video and then run the cropdetect filter on it, and return the most common crop parameters over that period.
    # I'm running this in WSL because I wasn't quite able to get it to work as I'd like using windows cmd or powershell.
    cmd = f'bash -c "ffmpeg -ss 120 -i \\"{bash_path}\\" -f matroska -t 300 -an -vf cropdetect=24:16:0 -y -crf 51 -preset ultrafast /dev/null 2>&1 | grep -o crop=.* | sort -bh | uniq -c | sort -bh | tail -n1 | grep -o crop=.* | sed \'s/.*crop=//g\'"'
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    errorcode = process.returncode
    if errorcode > 0:
        print(
            "cmd failed during get_crop_parameters, see above for details. Command: ",
            cmd,
        )
        exit(1)

    possible_crops = process.stdout.decode().splitlines()
    # Get most repetead resolution, that's the one with more possibilites to be right
    crop = max(possible_crops, key=possible_crops.count)
    width, height, _, _ = crop.split(":")
    info_log.debug(f"Crop parameters found: {width}x{height}")

    return width, height


def sonarr_main():
    # Import the CSV file with the list of series with manually specified crop parameters
    show_database = pd.read_csv(TV_file_path)
    movie_database = pd.read_csv(Movie_file_path)

    # Import the relevant environment variables
    events_vars = {variable: os.getenv(variable, False) for variable in variables}

    # log the event and set the relevant variables based on the event type
    if "sonarr_eventtype":
        event_is = "Sonarr"
    
        # set the relevant variables based on the event type
        file_path = events_vars["sonarr_episodefile_path"]
        source_path = events_vars["sonarr_episodefile_sourcepath"]
        name = events_vars["sonarr_series_title"]
        season = events_vars["sonarr_episodefile_seasonnumber"]
        episode = events_vars["sonarr_episodefile_episodenumbers"]

        # Check to see if the video is in the list of series with manually specified crop parameters
        database_results = show_database[(show_database["Series"] == name) & (show_database["Season"] == int(season))]

        # log the event
        info_log.info(
            f"New show added: {name} S{season}E{episode}\nSonarr_file_path => {file_path}\nsonarr_episodefile_sourcepath => {source_path}"
        )

    elif "radarr_eventtype":
        event_is = "Radarr"

        file_path = events_vars["radarr_moviefile_path"]
        source_path = events_vars["radarr_moviefile_sourcepath"]
        name = events_vars["radarr_movie_title"]
        database_results = movie_database[show_database["Movie"] == name]

        # log the event
        info_log.info(
            f"New movie added: {name}\nSonarr_file_path => {file_path}\nsonarr_episodefile_sourcepath => {source_path}"
        )

    # Process the video
    # Check to see if the source and destination file exist
    if os.path.exists(file_path) and os.path.exists(source_path):
        info_log.debug(f"Video files paths are valid.")

        # If it is, use the manually specified crop parameters
        if len(database_results) == 1:
            if event_is == "Sonarr":
                info_log.debug(
                    f"{name} S{season}E{episode} found in datatbase with {database_results['Horizontal'].values[0]}x{database_results['Vertical'].values[0]} resolution."
                )
            elif event_is == "Radarr":
                info_log.debug(
                    f"{name} found in datatbase with {database_results['Horizontal'].values[0]}x{database_results['Vertical'].values[0]} resolution."
                )

            # check to see if the video is already cropped
            already_croped = check_video_resolution(
                source_path,
                database_results["Horizontal"].values[0],
                database_results["Vertical"].values[0],
            )
            print(f"already_croped: {already_croped}")
            # if the video is already cropped, skip it
            if already_croped:
                info_log.info("Video already in the specified resolution. Skipping...")

            # else crop the video from the source to the destination
            else:
                info_log.info("Begining crop video!")
                crop_video(
                    source_path,
                    file_path,
                    database_results["Horizontal"].values[0],
                    database_results["Vertical"].values[0],
                )

        # If the video isn't in the show_database get the crop parameters automatically
        else:
            if event_is == "Sonarr":
                info_log.debug(
                    f"{name} S{season}E{episode} not found in datatbase. Getting crop parameters automatically."
                )

            # get the crop parameters
            width, height = get_crop_parameters(source_path)

            # check to see if the video is already cropped
            already_croped = check_video_resolution(
                source_path, width, height,
            )

            # Crop the video if it isn't already cropped
            if not already_croped:
                crop_video(
                    source_path,
                    file_path,
                    width,
                    height,
                )
    else:
        error_log.warning(
            f"Unable to find specified file(s). File_path => {file_path}\nsourcepath => {source_path}"
        )
        exit(1)


if __name__ == "__main__":
    EventTypes = [os.getenv("sonarr_eventtype"), os.getenv("radarr_eventtype")]
    if "Test" in EventTypes:
        info_log.info("Test has been ran successfully.")
        exit(0)
    sonarr_main()
    info_log.info("Completed script!")
