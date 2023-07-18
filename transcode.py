import os, ffmpeg, json
from multiprocessing import Pool


# load config
with open("config.json") as f:
    config = json.load(f)
    IN_DIR_LIST = config["in_dir_list"]
    OUT_DIR = config["out_dir"]
    LOW_QUALITY_ROOMIDS = config["low_quality_roomids"]


def transcode(in_file: str, out_file: str):
    assert in_file != out_file
    # probe the input file
    probe = ffmpeg.probe(in_file)
    video_bitrate, audio_bitrate = 0, 0
    for stream in probe["streams"]:
        if stream["codec_type"] == "video":
            video_bitrate = int(stream["bit_rate"])
        elif stream["codec_type"] == "audio":
            audio_bitrate = int(stream["bit_rate"])
    # get quality factor based on roomid
    roomid = os.path.basename(in_file).split("_")[0]
    if roomid in LOW_QUALITY_ROOMIDS:
        quality_factor = 51
    else:
        quality_factor = 30
    ffmpeg.input(in_file, hwaccel="cuda", hwaccel_output_format="cuda").output(
        out_file, vcodec="av1_nvenc", cq=quality_factor, acodec="copy"
    ).run(capture_stdout=False, capture_stderr=False, overwrite_output=True)


# loop over all files in the directory list
for dir in IN_DIR_LIST:
    for file in os.listdir(dir):
        base_name, ext = os.path.splitext(file)
        if ext in [".flv", ".mp4", ".m4v"]:
            # get the full path of the file
            in_file = os.path.join(dir, file)
            # create the output file name
            out_file = os.path.join(OUT_DIR, base_name + ".mp4")
            # transcode
            transcode(in_file, out_file)
